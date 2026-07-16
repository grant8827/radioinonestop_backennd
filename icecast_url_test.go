package main

import "testing"

func TestPublicIcecastListenURLDefaultsToStreamingHost(t *testing.T) {
	t.Setenv("ICECAST_PUBLIC_URL", "")
	got := publicIcecastListenURL("station-key")
	want := "https://stream.radioinonestop.com/station-key"
	if got != want {
		t.Fatalf("publicIcecastListenURL() = %q, want %q", got, want)
	}
}

func TestPublicIcecastListenURLHonorsConfiguredProxy(t *testing.T) {
	t.Setenv("ICECAST_PUBLIC_URL", "/icecast/")
	got := publicIcecastListenURL("station-key")
	want := "/icecast/station-key"
	if got != want {
		t.Fatalf("publicIcecastListenURL() = %q, want %q", got, want)
	}
}
