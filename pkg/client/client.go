package client

import (
	"crypto/tls"
	"github.com/arangodb/go-driver"
	"github.com/arangodb/go-driver/vst"
	"github.com/pkg/errors"
	"net/url"
)

// NewClient creates new client to the provided endpoints.
func NewClient(endpoints []string, auth driver.Authentication) (driver.Client, error) {

	var tlsConfig *tls.Config

	for i, endpoint := range endpoints {
		if i == 0 {
			if u, err := url.Parse(endpoint); err != nil {
				return nil, errors.Wrapf(err, "can not parse endpoint: %s", endpoint)
			} else {
				if u.Scheme == "https" || u.Scheme == "ssl" {
					tlsConfig = &tls.Config{InsecureSkipVerify: true}
				}
			}
		}
		//fmt.Printf("%d %s\n", i, endpoint)
	}

	conn, err := vst.NewConnection(vst.ConnectionConfig{
		Endpoints: endpoints,
		TLSConfig: tlsConfig,
	})
	if err != nil {
		return nil, errors.Wrap(err, "could not create connection")
	}

	client, err := driver.NewClient(driver.ClientConfig{
		Connection:     conn,
		Authentication: auth,
	})
	if err != nil {
		return nil, errors.Wrap(err, "could not create client")
	}

	return client, nil
}
