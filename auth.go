package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/xrpc"
	"github.com/golang-jwt/jwt/v4"
)

// Global constants and variables
const (
	authFile = "bsky_auth.json"
)

type BskyClient struct {
	mu             sync.RWMutex
	client         *xrpc.Client
	tokenExpiresAt time.Time
	saveDir        string
}

func NewBskyClient(ctx context.Context, saveDir string) (*BskyClient, error) {
	b := &BskyClient{
		client:          &xrpc.Client{Host: "https://bsky.social"},
		saveDir: saveDir,
	}

	if err := b.recoverySession(ctx); err != nil {
		return nil, err
	}

	return b, nil
}

func (b *BskyClient) recoverySession(ctx context.Context) error {
	authPath := filepath.Join(b.saveDir, authFile)
	if authData, err := os.ReadFile(authPath); err == nil {
		var auth xrpc.AuthInfo
		if err := json.Unmarshal(authData, &auth); err == nil {
			b.client.Auth = &auth
			b.updateTokenExpiration()
			log.Println("loaded existing session from file.")
			return b.refreshSession(ctx)
		} else {
			fmt.Errorf("failed to unmarchal json: %w", err)
			// fall through
		}
	} else {
		fmt.Errorf("failed to read file: %w", err)
		// fall through
	}
	// attempt full login
	return b.login(ctx)
}

func (b *BskyClient) refreshSession(ctx context.Context) error {
	if b.tokenExpiresAt.Before(time.Now().Add(5 * time.Minute)) {
		log.Println("attempting to refresh access token due to expiration...")
		// use refresh token as access token for the "ServerRefreshSession" call
		b.client.Auth.AccessJwt = b.client.Auth.RefreshJwt
		refreshCtx, cancel := context.WithTimeout(ctx, 60 * time.Second)
		defer cancel()
		renewed, err := atproto.ServerRefreshSession(refreshCtx, b.client)
		if err == nil {
			b.client.Auth.AccessJwt = renewed.AccessJwt
			b.client.Auth.RefreshJwt = renewed.RefreshJwt
			b.updateTokenExpiration()
			if err := b.saveAuthInfo(); err != nil {
				return fmt.Errorf("failed to save auth info: %w", err)
			}
			log.Println("session refreshed successfully.")
		} else {
			log.Printf("session refresh failed: %v. attempting full re-login...", err)
			if err := b.login(ctx); err != nil {
				return fmt.Errorf("failed to re-login: %w", err)
			}
		}
	}
	return nil
}

func (b *BskyClient) login(ctx context.Context) error {
	handle := os.Getenv("BSKY_ID")
	password := os.Getenv("BSKY_APPPASSWORD")
	if handle == "" || password == "" {
		return fmt.Errorf("BSKY_ID and BSKY_APPPASSWORD environment variables must be set")
	}

	createCtx, cancel := context.WithTimeout(ctx, 60 * time.Second)
	defer cancel()
	output, err := atproto.ServerCreateSession(createCtx, b.client, &atproto.ServerCreateSession_Input{
		Identifier: handle,
		Password:   password,
	})
	if err != nil {
		return fmt.Errorf("failed to create session: %w", err)
	}

	b.client.Auth = &xrpc.AuthInfo{
		Handle:     output.Handle,
		Did:        output.Did,
		AccessJwt:  output.AccessJwt,
		RefreshJwt: output.RefreshJwt,
	}
	b.updateTokenExpiration()
	log.Println("new session created.")
	return b.saveAuthInfo()
}

func (b *BskyClient) updateTokenExpiration() {
	if b.client.Auth == nil {
		return
	}
	token, _ := jwt.Parse(b.client.Auth.AccessJwt, nil)
	if claims, ok := token.Claims.(jwt.MapClaims); ok {
		if exp, ok := claims["exp"].(float64); ok {
			b.tokenExpiresAt = time.Unix(int64(exp), 0)
		}
	}
}

func (b *BskyClient) saveAuthInfo() error {
	authJson, err := json.Marshal(b.client.Auth)
	if err != nil {
		return fmt.Errorf("failed to marshal auth info: %w", err)
	}
	authPath := filepath.Join(b.saveDir, authFile)
	if err := os.WriteFile(authPath, authJson, 0644); err != nil {
		return fmt.Errorf("failed to write auth file: %w", err)
	}
	return nil
}

// GetClient provides a thread-safe way to get the client instance.
func (b *BskyClient) GetClient(ctx context.Context) *xrpc.Client {
    // lock
    b.mu.RLock()
    defer b.mu.RUnlock()

	if err := b.refreshSession(ctx); err != nil {
		log.Fatalf("cannot refresh session...aborted.")
	}
	return b.client
}
