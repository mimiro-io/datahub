package middlewares

import (
	"context"
	"log"
	"os"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v4"
	"github.com/mimiro-io/datahub/internal/security"
	tc "github.com/testcontainers/testcontainers-go/modules/compose"
)

func TestMain(m *testing.M) {
	compose, composeErr := tc.NewDockerCompose("./test/docker-compose.yml")
	if composeErr != nil {
		log.Fatal(composeErr)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	upErr := compose.Up(ctx, tc.Wait(true))
	if upErr != nil {
		log.Fatal(upErr)
	}

	log.Println("Testcontainers UP:", compose.Services())

	//Run the test suite
	exitCode := m.Run()

	defer func() {
		downErr := compose.Down(ctx, tc.RemoveOrphans(true), tc.RemoveImagesLocal)
		if downErr != nil {
			log.Fatal(downErr)
		}

		log.Println("Testcontainers DOWN:", compose.Services())
	}()

	log.Println("Tests exited:", exitCode)

	os.Exit(exitCode)
}

func testJwt(t *testing.T) *jwt.Token {
	t.Helper()
	return &jwt.Token{
		Raw:    "eyJhbGciOiJSUzI1NiIsImtpZCI6ImI3MGZlZjg4LWU4MjYtNGYzZS04ZmNmLWZjYTJmNzBlZmY1ZSIsInR5cCI6IkpXVCJ9.eyJhdWQiOltdLCJjbGllbnRfaWQiOiI0NjI2ODAxNS04NTE2LTRjM2ItYjAxZi1kY2MzN2ViOWE4ZDciLCJlbWFpbCI6ImRhNzdkZWEzYWFmYTQ0MTVAOGVjZGVjY2NmM2EwYjU2Yy5ubyIsImV4cCI6MTcyNDI0MzI2MCwiZXh0Ijp7ImVtYWlsIjoiZGE3N2RlYTNhYWZhNDQxNUA4ZWNkZWNjY2YzYTBiNTZjLm5vIiwiZ3R5IjoicmVmcmVzaF90b2tlbiIsInNjb3BlIjoic3VwcG9ydDpyIHN1cHBvcnQ6dyJ9LCJndHkiOiJyZWZyZXNoX3Rva2VuIiwiaWF0IjoxNzI0MjM5NjYwLCJpc3MiOiJodHRwczovL2F1dGguZGV2Lm1pbWlyby5pbyIsImp0aSI6IjVjMzRhMzEwLWFkM2ItNDE2Mi1iOWFkLTA5ZjJiODNlYWE5ZSIsIm5iZiI6MTcyNDIzOTY2MCwic2NvcGUiOiJzdXBwb3J0OnIgc3VwcG9ydDp3Iiwic2NwIjpbIm9wZW5pZCIsIm9mZmxpbmUiXSwic3ViIjoiYTVjMjAyY2QtYmM3Yi00Y2QyLWE2ZGEtZWQ3M2QwMjk0ZTE0In0.WlGwhKaVgOg4u9kk7xsumHMyrOfUXEy84qJ9rrsNBtl6rnysF6qNPybYclbhtsmc_TVBBLmuVYndHdp991zJ-9jUCtVo9Ps5J3irVNk7nwUnaz6yk9opP_wpmuSTxAYbeW7D8w-3sFQMC72kksOPs_T5AJMFashqzUhI4VLF045ipOPTi50tPbshh-8Z4u45d_J1LfrYNHUKvGW3bV0G3b_sI0bNhkyBb0r7_JCTAynn99cg0wpd2aOMzT0VtHOJRhvC7COvKkW2jCFn5ABX109qQyQNdUSuHRctD-4WVSFVO6VQKrzNQH2qwG-BylTvtYSp9oFoj9h4WBy0H4y_mX5-okjCD99UJRTjfQ--V-XDLoSstcKy_aPAGreKoZsFexewo3i64N0v95RfnfAYiDHxi8YYpwXlEFN1y2JL-Y612ZuaOz_6qX7qbkbHsCQIZ1GZeWGPnoq124n4NuiN_pl9nXTI5D17OMuzbRD2ReqY4ps_xLBVW6vlknUW4cBgvIK54g-lfnzvXVTja2pWPPyAuMF0uDXV5kpj69rH-rPb6Ch_T1JJfO1j6eHoqZbxgrqPAtvbmWPlnRXnv-Zs2GDnvIyxMLK89Bjd_LtqSFxOhYuvc4osTFeTm536H7BNMF6r-G_5Ztt56_neJ0BRzFcdVA5CaQF7k3icch1l0Ps",
		Method: jwt.SigningMethodRS256,
		Header: nil,
		Claims: security.CustomClaims{
			Scope:    "support:r support:w",
			Scp:      []string{"openid", "offline"},
			Gty:      "client-credentials",
			Adm:      true,
			Roles:    nil,
			ClientID: "a5c202cd-bc7b-4cd2-a6da-ed73d0294e14",
			RegisteredClaims: jwt.RegisteredClaims{
				Issuer:    "https://auth.dev.mimiro.io",
				Subject:   "a5c202cd-bc7b-4cd2-a6da-ed73d0294e14",
				Audience:  jwt.ClaimStrings{},
				ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
				IssuedAt:  jwt.NewNumericDate(time.Now()),
			},
		},
		Signature: "",
		Valid:     true,
	}
}

func Test_doOpaCheck_GET_403(t *testing.T) {
	_, err := doOpaCheck(
		"GET",
		"/jobs",
		testJwt(t),
		[]string{"read"},
		"http://localhost:8181")

	if err == nil {
		t.Fatalf("Forbidden expected")
	}

	t.Logf("Got expected error: %v", err)
}

func Test_doOpaCheck_POST_403(t *testing.T) {
	_, err := doOpaCheck(
		"POST",
		"/datasets/tinegrass.PredictionResult/entities",
		testJwt(t),
		[]string{"write"},
		"http://localhost:8181")

	if err == nil {
		t.Fatalf("Forbidden expected")
	}

	t.Logf("Got expected error: %v", err)
}
