package main

import (
	"cmp"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/bluesky-social/indigo/api/atproto"
	"github.com/bluesky-social/indigo/api/bsky"
	"github.com/bluesky-social/indigo/lex/util"
	"github.com/bluesky-social/jetstream/pkg/client"
	"github.com/bluesky-social/jetstream/pkg/client/schedulers/sequential"
	"github.com/bluesky-social/jetstream/pkg/models"

	"github.com/trubitsyn/go-zero-width"
)

// Global constants and variables
const (
	timeRFC3339Millis = "2006-01-02T15:04:05.000Z07:00"
	maxLength         = 300
	lastRankFile      = "last_rank.json"
)
var /* constant */ (
	wantedCollections = []string{"app.bsky.feed.post"}
	hashTagsRegexp    = regexp.MustCompile(`#\S+`)
)

// Double linked list ======================
type Node struct {
	tag string
	timeUs time.Time
	prev *Node
	next *Node
}

type LinkedList struct {
	mu   sync.RWMutex // protect to modify
	head *Node
	tail *Node
}

func (ll *LinkedList) Append(tag string, timeUs time.Time) {
	// lock
	ll.mu.RLock()
	defer ll.mu.RUnlock()

	newNode := &Node{tag: tag, timeUs: timeUs}
	if ll.head == nil {
		ll.head = newNode
		ll.tail = newNode
	} else {
		current := ll.tail
		newNode.prev = current
		current.next = newNode
		ll.tail = newNode
	}
}

func (ll *LinkedList) Remove(node *Node) {
	// lock
	ll.mu.RLock()
	defer ll.mu.RUnlock()

	prev := node.prev
	next := node.next
	if prev != nil {
		prev.next = next
	}
	if next != nil {
		next.prev = prev
	}
	if ll.tail == node {
		ll.tail = prev
	}
	if ll.head == node {
		ll.head = next
	}
}

// Make subscript number string ===========
var /* constant */ mapsub = map[rune]string{
	'0': "₀",
	'1': "₁",
	'2': "₂",
	'3': "₃",
	'4': "₄",
	'5': "₅",
	'6': "₆",
	'7': "₇",
	'8': "₈",
	'9': "₉",
}

func makeSubNum(i int) string {
	src := fmt.Sprintf("%d", i)
	var b strings.Builder
	for _, d := range []rune(src) {
		b.WriteString(mapsub[d])
	}
	return b.String()
}

// remove unnecessary chars ===============
func trimAll(src string) string {
	return strings.TrimFunc(zerowidth.RemoveZeroWidthCharacters(strings.TrimSpace(src)), func(r rune) bool {
		return unicode.In(r, unicode.Variation_Selector)
	})
}

// Custom jetstream client ========================
type handler struct {
	saveDir      string
	numOfPost    int
	duration     time.Duration
	postInterval time.Duration
	lang         string
	did          string
	tags         *LinkedList
	latest       time.Time
	lastRank     map[string]int
	b            *BskyClient
}

func (h *handler) HandleEvent(ctx context.Context, event *models.Event) error {
	// Unmarshal the record if there is one
	if event.Commit != nil && (event.Commit.Operation == models.CommitOperationCreate || event.Commit.Operation == models.CommitOperationUpdate) {
		if event.Did == h.did {
			log.Printf("skip post by myself")
			return nil
		}
		switch event.Commit.Collection {
		case "app.bsky.feed.post":
			var post bsky.FeedPost
			if err := json.Unmarshal(event.Commit.Record, &post); err != nil {
				log.Printf("failed to unmarshal post: %v", err)
				return nil
			}
			// extract hashtags
			if slices.Contains(post.Langs, h.lang) {
				for _, facet := range post.Facets {
					for _, feat := range facet.Features {
						if feat.RichtextFacet_Tag != nil {
							cleaned := fmt.Sprintf("#%s", trimAll(feat.RichtextFacet_Tag.Tag))
							t := time.UnixMicro(event.TimeUS);
							h.tags.Append(cleaned, t)
							log.Printf("%v, %v", cleaned, t)
							h.latest = t
						}
					}
				}
			}
		}
	}
	return nil
}

func makeFacets(text string) []*bsky.RichtextFacet {
	bytes := []byte(text)
	matches := hashTagsRegexp.FindAllIndex(bytes, -1)
	facets := make([]*bsky.RichtextFacet, 0, len(matches))
	for _, match := range matches {
		facets = append(facets, &bsky.RichtextFacet{
			Features: []*bsky.RichtextFacet_Features_Elem{
				&bsky.RichtextFacet_Features_Elem{
					RichtextFacet_Tag: &bsky.RichtextFacet_Tag{
						Tag: string(bytes[match[0] + 1:match[1]]),
					},
				},
			},
			Index: &bsky.RichtextFacet_ByteSlice{
				ByteStart: int64(match[0]),
				ByteEnd: int64(match[1]),
			},
		})
	}
	return facets
}

func (h *handler) sendPost(ctx context.Context, text, ruri, rcid, puri, pcid string) (string, string, error) {
	var post *bsky.FeedPost
	if ruri != "" {
		post = &bsky.FeedPost{
			Text: text,
			CreatedAt: time.Now().UTC().Format(timeRFC3339Millis),
			Langs: []string{h.lang},
			Facets: makeFacets(text),
			Reply: &bsky.FeedPost_ReplyRef{
				Root: &atproto.RepoStrongRef{
					Uri: ruri,
					Cid: rcid,
				},
				Parent: &atproto.RepoStrongRef{
					Uri: puri,
					Cid: pcid,
				},
			},
		}
	} else {
		post = &bsky.FeedPost{
			Text: text,
			CreatedAt: time.Now().UTC().Format(timeRFC3339Millis),
			Langs: []string{h.lang},
			Facets: makeFacets(text),
		}
	}
	client := h.b.GetClient(ctx)
	sendPostCtx, cancel := context.WithTimeout(ctx, 1 * time.Minute)
	defer cancel()
	res, err := atproto.RepoCreateRecord(
		sendPostCtx,
		client,
		&atproto.RepoCreateRecord_Input{
			Repo: client.Auth.Did,
			Collection: "app.bsky.feed.post",
			Record: &util.LexiconTypeDecoder{Val: post},
		},
	)
	if err != nil {
		return "", "", fmt.Errorf("cannot post \"%s\". %w", text, err)
	}
	return res.Uri, res.Cid, nil
}

func (h *handler) getTrend(tag string, rank int) string {
	if last, ok := h.lastRank[tag]; ok {
		d := last - rank
		if d > 10 {
			return "⬆️"
		} else if d > 0 {
			return "↗️"
		} else if d < -10 {
			return "⬇️"
		} else if d < 0 {
			return "↘️"
		} else { // same rank
			return "➡️"
		}
	} else {
		return "⬆️"
	}
}

func (h *handler) cleanCountPost(ctx context.Context) {
	// infinite loop
	for {
		c := make(map[string]int)
		d := time.Now().Truncate(h.postInterval).Add(h.postInterval).Sub(time.Now())
		dispTags := make(map[string]string)
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping the process...")
			return
		case <-time.After(d): // wait
			e := time.Now().Add(-h.duration)
			node := h.tags.head
			for node != nil {
				if node.timeUs.Before(e) {
					log.Printf("Remove %v, %v", node.tag, node.timeUs)
					h.tags.Remove(node)
				} else {
					log.Printf("Count %v, %v", node.tag, node.timeUs)
					lowerTag := strings.ToLower(node.tag)
					c[lowerTag]++
					if _, ok := dispTags[lowerTag]; !ok || node.tag != lowerTag {
						dispTags[lowerTag] = node.tag
					}
				}
				node = node.next
			}
		}
		sortedTags := slices.SortedStableFunc(maps.Keys(c), func(i, j string) int {
			// NOTE: this is for reverse sort
			if c[i] == c[j] {
				// 文字数カウント
				ri := []rune(i)
				rj := []rune(j)
				if len(ri) == len(rj) {
					return -strings.Compare(i, j)
				} else {
					return -cmp.Compare(len(ri), len(rj))
				}
			} else {
				return -cmp.Compare(c[i], c[j])
			}
		})
		j := 0
		t := ""
		rank := make(map[string]int)
		var ruri, rcid, puri, pcid string
		var err error
		for i, k := range sortedTags {
			rank[k] = i
			w := fmt.Sprintf("%s%d. %s %s", h.getTrend(k, i), i + 1, dispTags[k], makeSubNum(c[k]))
			if (len([]rune(t)) + len([]rune(w))) >= maxLength {
				log.Printf("%s", t)
				puri, pcid, err = h.sendPost(ctx, t, ruri, rcid, puri, pcid)
				if err != nil {
					log.Printf("fail to sendPost. %w", err)
				}
				if ruri == "" {
					ruri = puri
					rcid = pcid
				}
				j++
				if j >= h.numOfPost {
					break
				}
				t = w
			} else if t == "" {
				t = w
			} else {
				t = t + "\n" + w
			}
		}
		if j < h.numOfPost && t != "" {
			log.Printf("%s", t)
			puri, pcid, err = h.sendPost(ctx, t, ruri, rcid, puri, pcid)
			if err != nil {
				log.Printf("fail to sendPost. %w", err)
			}
		}
		h.saveLastRank(rank)
	}
}

func (h *handler) loadLastRank() {
	lastRankPath := filepath.Join(h.saveDir, lastRankFile)
	if lastRankData, err := os.ReadFile(lastRankPath); err == nil {
		if err := json.Unmarshal(lastRankData, &h.lastRank); err != nil {
			log.Printf("failed to unmarshal %s: %w", lastRankFile, err)
		}
	} else {
		log.Printf("failed to read %s: %w", lastRankFile, err)
	}
	// any errors ignored
}

func (h *handler) saveLastRank(rank map[string]int) {
	h.lastRank = rank
	if lastRankJson, err := json.Marshal(h.lastRank); err == nil {
		lastRankPath := filepath.Join(h.saveDir, lastRankFile)
		if err := os.WriteFile(lastRankPath, lastRankJson, 0644); err != nil {
			log.Printf("failed to write %s: %w", lastRankFile, err)
		}
	} else {
		log.Printf("failed to marshal last rank: %w", err)
	}
	// any errors ignored
}

func main() {
	// command-line flags and defaults
	jetstreamUrl := *flag.String("jsUrl", "wss://jetstream2.us-west.bsky.network/subscribe", "Bluesky Jetstream URL")
	numOfPost := *flag.Int("numOfPost", 3, "Number of posts once")
	duration := time.Duration(*flag.Int("periodSec", 7200, "Measurement period in seconds")) * time.Second
	postInterval := time.Duration(*flag.Int("intervalSec", 900, "Post interval in seconds")) * time.Second
	saveDir := *flag.String("saveDir", "./work", "Directory to save session info")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	logger := slog.Default()

	// bsky client setup
	bskyClient, err := NewBskyClient(ctx, saveDir)
	if err != nil {
		log.Fatalf("failed to initialize Bluesky client: %v", err)
	}
	bc := bskyClient.GetClient(ctx)
	log.Printf("bskyClient.client.Auth.Did: %v", bc.Auth.Did)

	h := &handler{
		saveDir: saveDir,
		numOfPost: numOfPost,
		duration: duration,
		postInterval: postInterval,
		lang: os.Getenv("TARGET_LANG"),
		did: bc.Auth.Did,
		tags: &LinkedList{},
		latest: time.Now().Add(-duration),
		b: bskyClient,
	}
	h.loadLastRank()

	scheduler := sequential.NewScheduler("jetstream_hashtagstrend", logger, h.HandleEvent)

	config := client.DefaultClientConfig()
	config.WebsocketURL = jetstreamUrl
	config.WantedCollections = wantedCollections
	config.Compress = true

	c, err := client.NewClient(config, logger, scheduler)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	// Every 5 seconds print the events read and bytes read and average event size
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		for {
			select {
			case <-ticker.C:
				eventsRead := c.EventsRead.Load()
				bytesRead := c.BytesRead.Load()
				avgEventSize := bytesRead / max(1, eventsRead)
				logger.Info("stats", "events_read", eventsRead, "bytes_read", bytesRead, "avg_event_size", avgEventSize)
			}
		}
	}()

	// periodically clean, count and post
	go h.cleanCountPost(ctx)

	for { // infinite loop
		select {
		case <-ctx.Done():
			log.Println("Context cancelled, stopping the process...")
			return
		case <-time.After(1 * time.Second):
			cursor := h.latest.UnixMicro()
			if err := c.ConnectAndRead(ctx, &cursor); err != nil {
				log.Printf("failed to connect: %v, restarting...", err)
			}
		}
	}
}
