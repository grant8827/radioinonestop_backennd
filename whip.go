package main

import (
	"io"
	"log"
	"net/http"

	"github.com/pion/webrtc/v4"
)

type WHIPController struct {
	MediaEngine *webrtc.MediaEngine
	API         *webrtc.API
}

func NewWHIPController() *WHIPController {
	m := &webrtc.MediaEngine{}
	// Enable standard H264 & Opus media codecs
	if err := m.RegisterDefaultCodecs(); err != nil {
		log.Printf("failed to register default codecs: %v", err)
	}

	api := webrtc.NewAPI(webrtc.WithMediaEngine(m))
	return &WHIPController{MediaEngine: m, API: api}
}

func (wc *WHIPController) HandleIngest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Only POST is supported for WHIP endpoints", http.StatusMethodNotAllowed)
		return
	}

	bodyBytes, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	sdpOffer := webrtc.SessionDescription{
		Type: webrtc.SDPTypeOffer,
		SDP:  string(bodyBytes),
	}

	peerConnection, err := wc.API.NewPeerConnection(webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: []string{"stun:stun.l.google.com:19302"}},
		},
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Read and forward incoming track data
	peerConnection.OnTrack(func(track *webrtc.TrackRemote, receiver *webrtc.RTPReceiver) {
		log.Printf("WHIP: Track received %s (%s)", track.ID(), track.Codec().MimeType)
		// Pipeline the RTP packets directly to FFmpeg compositing process or RTMP forwarder
		go pipelineRTPToFFmpeg(track)
	})

	peerConnection.OnICEConnectionStateChange(func(connectionState webrtc.ICEConnectionState) {
		log.Printf("WHIP: ICE Connection State has changed: %s", connectionState.String())
	})

	if err := peerConnection.SetRemoteDescription(sdpOffer); err != nil {
		log.Printf("WHIP SetRemoteDescription error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Gather ICE candidates automatically (Pion trick)
	gatherComplete := webrtc.GatheringCompletePromise(peerConnection)

	answer, err := peerConnection.CreateAnswer(nil)
	if err != nil {
		log.Printf("WHIP CreateAnswer error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := peerConnection.SetLocalDescription(answer); err != nil {
		log.Printf("WHIP SetLocalDescription error: %v", err)
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	<-gatherComplete

	w.Header().Set("Content-Type", "application/sdp")
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(peerConnection.LocalDescription().SDP))
}

func pipelineRTPToFFmpeg(track *webrtc.TrackRemote) {
	// 1. Establish an OS subprocess call to FFmpeg
	// 2. Read packets in a loop: track.ReadRTP()
	// 3. Write payload data to FFmpeg's standard input pipe
	for {
		_, _, err := track.ReadRTP()
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("WHIP ReadRTP error: %v", err)
			return
		}
		// In the actual compositing engine, we write to an FFmpeg pipeline here
	}
}
