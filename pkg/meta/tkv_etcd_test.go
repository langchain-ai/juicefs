//go:build !noetcd

/*
 * JuiceFS, Copyright 2026 Juicedata, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package meta

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"math/big"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBuildTLSConfig(t *testing.T) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	ca := &x509.Certificate{
		SerialNumber: big.NewInt(1), NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	leaf := &x509.Certificate{
		SerialNumber: big.NewInt(2), NotBefore: ca.NotBefore, NotAfter: ca.NotAfter,
		DNSNames: []string{"etcd.test"}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	writePEM := func(name, kind string, der []byte) string {
		t.Helper()
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}), 0600); err != nil {
			t.Fatal(err)
		}
		return path
	}
	caFile := writePEM("ca.pem", "CERTIFICATE", caDER)
	certFile := writePEM("cert.pem", "CERTIFICATE", certDER)
	keyFile := writePEM("key.pem", "EC PRIVATE KEY", keyDER)
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	parsedCA, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	roots.AddCert(parsedCA)
	for _, tt := range []struct {
		name                                                 string
		params                                               url.Values
		wantNil, wantErr, hostnameErr, unknownCA, clientAuth bool
	}{
		{name: "no TLS parameters", wantNil: true},
		{name: "missing CA", params: url.Values{"cacert": {filepath.Join(dir, "missing.pem")}}, wantErr: true},
		{name: "certificate without key", params: url.Values{"cert": {certFile}}, wantErr: true},
		{name: "key without certificate", params: url.Values{"key": {keyFile}}, wantErr: true},
		{name: "invalid key pair", params: url.Values{"cert": {certFile}, "key": {caFile}}, wantErr: true},
		{name: "custom CA", params: url.Values{"cacert": {caFile}, "server-name": {"etcd.test"}}},
		{name: "untrusted CA", params: url.Values{"server-name": {"etcd.test"}}, unknownCA: true},
		{name: "wrong hostname", params: url.Values{"cacert": {caFile}, "server-name": {"wrong.test"}}, hostnameErr: true},
		{name: "client certificate", params: url.Values{"cacert": {caFile}, "server-name": {"etcd.test"}, "cert": {certFile}, "key": {keyFile}}, clientAuth: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			config, err := buildTlsConfig(&url.URL{RawQuery: tt.params.Encode()})
			if (err != nil) != tt.wantErr {
				t.Fatalf("buildTlsConfig error = %v, want error %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}
			if (config == nil) != tt.wantNil {
				t.Fatalf("config = %v, want nil %v", config, tt.wantNil)
			}
			if tt.wantNil {
				return
			}
			if config.InsecureSkipVerify {
				t.Fatal("certificate verification disabled")
			}
			serverConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
			if tt.clientAuth {
				serverConfig.ClientAuth = tls.RequireAndVerifyClientCert
				serverConfig.ClientCAs = roots
			}
			clientConn, serverConn := net.Pipe()
			defer clientConn.Close()
			defer serverConn.Close()
			deadline := time.Now().Add(5 * time.Second)
			if err := clientConn.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			if err := serverConn.SetDeadline(deadline); err != nil {
				t.Fatal(err)
			}
			serverResult := make(chan error, 1)
			go func() {
				serverResult <- tls.Server(serverConn, serverConfig).Handshake()
				serverConn.Close()
			}()
			err = tls.Client(clientConn, config).Handshake()
			clientConn.Close()
			serverErr := <-serverResult
			switch {
			case tt.hostnameErr:
				var hostnameErr x509.HostnameError
				if !errors.As(err, &hostnameErr) {
					t.Fatalf("handshake error = %v, want hostname error", err)
				}
			case tt.unknownCA:
				var authorityErr x509.UnknownAuthorityError
				if !errors.As(err, &authorityErr) {
					t.Fatalf("handshake error = %v, want unknown CA", err)
				}
			default:
				if err != nil || serverErr != nil {
					t.Fatalf("handshake errors: client=%v server=%v", err, serverErr)
				}
			}
		})
	}
}
