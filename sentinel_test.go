//go:build !skiptest

package dicedb_test

import (
	"context"
	"net"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/dicedb/dicedb-go"
)

var _ = Describe("Sentinel PROTO 2", func() {
	var client *dicedb.Client

	BeforeEach(func() {
		client = dicedb.NewFailoverClient(&dicedb.FailoverOptions{
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,
			MaxRetries:    -1,
			Protocol:      2,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		_ = client.Close()
	})

	It("should sentinel client PROTO 2", func() {
		val, err := client.Do(ctx, "HELLO").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).Should(ContainElements("proto", int64(2)))
	})
})

var _ = Describe("Sentinel", func() {
	var client *dicedb.Client
	var master *dicedb.Client
	var masterPort string
	var sentinel *dicedb.SentinelClient

	BeforeEach(func() {
		client = dicedb.NewFailoverClient(&dicedb.FailoverOptions{
			ClientName:    "sentinel_hi",
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,
			MaxRetries:    -1,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		sentinel = dicedb.NewSentinelClient(&dicedb.Options{
			Addr:       ":" + sentinelPort1,
			MaxRetries: -1,
		})

		addr, err := sentinel.GetMasterAddrByName(ctx, sentinelName).Result()
		Expect(err).NotTo(HaveOccurred())

		master = dicedb.NewClient(&dicedb.Options{
			Addr:       net.JoinHostPort(addr[0], addr[1]),
			MaxRetries: -1,
		})
		masterPort = addr[1]

		// Wait until slaves are picked up by sentinel.
		Eventually(func() string {
			return sentinel1.Info(ctx).Val()
		}, "15s", "100ms").Should(ContainSubstring("slaves=2"))
		Eventually(func() string {
			return sentinel2.Info(ctx).Val()
		}, "15s", "100ms").Should(ContainSubstring("slaves=2"))
		Eventually(func() string {
			return sentinel3.Info(ctx).Val()
		}, "15s", "100ms").Should(ContainSubstring("slaves=2"))
	})

	AfterEach(func() {
		_ = client.Close()
		_ = master.Close()
		_ = sentinel.Close()
	})

	It("should facilitate failover", func() {
		// Set value on master.
		err := client.Set(ctx, "foo", "master", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		// Verify.
		val, err := client.Get(ctx, "foo").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("master"))

		// Verify master->slaves sync.
		var slavesAddr []string
		Eventually(func() []string {
			slavesAddr = dicedb.GetSlavesAddrByName(ctx, sentinel, sentinelName)
			return slavesAddr
		}, "15s", "100ms").Should(HaveLen(2))
		Eventually(func() bool {
			sync := true
			for _, addr := range slavesAddr {
				slave := dicedb.NewClient(&dicedb.Options{
					Addr:       addr,
					MaxRetries: -1,
				})
				sync = slave.Get(ctx, "foo").Val() == "master"
				_ = slave.Close()
			}
			return sync
		}, "15s", "100ms").Should(BeTrue())

		// Create subscription.
		pub := client.Subscribe(ctx, "foo")
		ch := pub.Channel()

		// Kill master.
		err = master.Shutdown(ctx).Err()
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() error {
			return master.Ping(ctx).Err()
		}, "15s", "100ms").Should(HaveOccurred())

		// Check that client picked up new master.
		Eventually(func() string {
			return client.Get(ctx, "foo").Val()
		}, "15s", "100ms").Should(Equal("master"))

		// Check if subscription is renewed.
		var msg *dicedb.Message
		Eventually(func() <-chan *dicedb.Message {
			_ = client.Publish(ctx, "foo", "hello").Err()
			return ch
		}, "15s", "100ms").Should(Receive(&msg))
		Expect(msg.Channel).To(Equal("foo"))
		Expect(msg.Payload).To(Equal("hello"))
		Expect(pub.Close()).NotTo(HaveOccurred())

		_, err = startRedis(masterPort)
		Expect(err).NotTo(HaveOccurred())
	})

	It("supports DB selection", func() {
		Expect(client.Close()).NotTo(HaveOccurred())

		client = dicedb.NewFailoverClient(&dicedb.FailoverOptions{
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,
			DB:            1,
		})
		err := client.Ping(ctx).Err()
		Expect(err).NotTo(HaveOccurred())
	})

	It("should sentinel client setname", func() {
		Expect(client.Ping(ctx).Err()).NotTo(HaveOccurred())
		val, err := client.ClientList(ctx).Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).Should(ContainSubstring("name=sentinel_hi"))
	})

	It("should sentinel client PROTO 3", func() {
		val, err := client.Do(ctx, "HELLO").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).Should(HaveKeyWithValue("proto", int64(3)))
	})
})

var _ = Describe("NewFailoverClusterClient PROTO 2", func() {
	var client *dicedb.ClusterClient

	BeforeEach(func() {
		client = dicedb.NewFailoverClusterClient(&dicedb.FailoverOptions{
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,
			Protocol:      2,

			RouteRandomly: true,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		_ = client.Close()
	})

	It("should sentinel cluster PROTO 2", func() {
		_ = client.ForEachShard(ctx, func(ctx context.Context, c *dicedb.Client) error {
			val, err := client.Do(ctx, "HELLO").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).Should(ContainElements("proto", int64(2)))
			return nil
		})
	})
})

var _ = Describe("NewFailoverClusterClient", func() {
	var client *dicedb.ClusterClient
	var master *dicedb.Client
	var masterPort string

	BeforeEach(func() {
		client = dicedb.NewFailoverClusterClient(&dicedb.FailoverOptions{
			ClientName:    "sentinel_cluster_hi",
			MasterName:    sentinelName,
			SentinelAddrs: sentinelAddrs,

			RouteRandomly: true,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		sentinel := dicedb.NewSentinelClient(&dicedb.Options{
			Addr:       ":" + sentinelPort1,
			MaxRetries: -1,
		})

		addr, err := sentinel.GetMasterAddrByName(ctx, sentinelName).Result()
		Expect(err).NotTo(HaveOccurred())

		master = dicedb.NewClient(&dicedb.Options{
			Addr:       net.JoinHostPort(addr[0], addr[1]),
			MaxRetries: -1,
		})
		masterPort = addr[1]

		// Wait until slaves are picked up by sentinel.
		Eventually(func() string {
			return sentinel1.Info(ctx).Val()
		}, "15s", "100ms").Should(ContainSubstring("slaves=2"))
		Eventually(func() string {
			return sentinel2.Info(ctx).Val()
		}, "15s", "100ms").Should(ContainSubstring("slaves=2"))
		Eventually(func() string {
			return sentinel3.Info(ctx).Val()
		}, "15s", "100ms").Should(ContainSubstring("slaves=2"))
	})

	AfterEach(func() {
		_ = client.Close()
		_ = master.Close()
	})

	It("should facilitate failover", func() {
		Skip("Flaky Test")
		// Set value.
		err := client.Set(ctx, "foo", "master", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		for i := 0; i < 100; i++ {
			// Verify.
			Eventually(func() string {
				return client.Get(ctx, "foo").Val()
			}, "15s", "1ms").Should(Equal("master"))
		}

		// Create subscription.
		sub := client.Subscribe(ctx, "foo")
		ch := sub.Channel()

		// Kill master.
		err = master.Shutdown(ctx).Err()
		Expect(err).NotTo(HaveOccurred())
		Eventually(func() error {
			return master.Ping(ctx).Err()
		}, "15s", "100ms").Should(HaveOccurred())

		// Check that client picked up new master.
		Eventually(func() string {
			return client.Get(ctx, "foo").Val()
		}, "15s", "100ms").Should(Equal("master"))

		// Check if subscription is renewed.
		var msg *dicedb.Message
		Eventually(func() <-chan *dicedb.Message {
			_ = client.Publish(ctx, "foo", "hello").Err()
			return ch
		}, "15s", "100ms").Should(Receive(&msg))
		Expect(msg.Channel).To(Equal("foo"))
		Expect(msg.Payload).To(Equal("hello"))
		Expect(sub.Close()).NotTo(HaveOccurred())

		_, err = startRedis(masterPort)
		Expect(err).NotTo(HaveOccurred())
	})

	It("should sentinel cluster client setname", func() {
		Skip("Flaky Test")
		err := client.ForEachShard(ctx, func(ctx context.Context, c *dicedb.Client) error {
			return c.Ping(ctx).Err()
		})
		Expect(err).NotTo(HaveOccurred())

		_ = client.ForEachShard(ctx, func(ctx context.Context, c *dicedb.Client) error {
			val, err := c.ClientList(ctx).Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).Should(ContainSubstring("name=sentinel_cluster_hi"))
			return nil
		})
	})

	It("should sentinel cluster PROTO 3", func() {
		Skip("Flaky Test")
		_ = client.ForEachShard(ctx, func(ctx context.Context, c *dicedb.Client) error {
			val, err := client.Do(ctx, "HELLO").Result()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).Should(HaveKeyWithValue("proto", int64(3)))
			return nil
		})
	})
})

var _ = Describe("SentinelAclAuth", func() {
	const (
		aclSentinelUsername = "sentinel-user"
		aclSentinelPassword = "sentinel-pass"
	)

	var client *dicedb.Client
	var sentinel *dicedb.SentinelClient
	sentinels := func() []*redisProcess {
		return []*redisProcess{sentinel1, sentinel2, sentinel3}
	}

	BeforeEach(func() {
		authCmd := dicedb.NewStatusCmd(ctx, "ACL", "SETUSER", aclSentinelUsername, "ON",
			">"+aclSentinelPassword, "-@all", "+auth", "+client|getname", "+client|id", "+client|setname",
			"+command", "+hello", "+ping", "+client|setinfo", "+role", "+sentinel|get-master-addr-by-name", "+sentinel|master",
			"+sentinel|myid", "+sentinel|replicas", "+sentinel|sentinels")

		for _, process := range sentinels() {
			err := process.Client.Process(ctx, authCmd)
			Expect(err).NotTo(HaveOccurred())
		}

		client = dicedb.NewFailoverClient(&dicedb.FailoverOptions{
			MasterName:       sentinelName,
			SentinelAddrs:    sentinelAddrs,
			MaxRetries:       -1,
			SentinelUsername: aclSentinelUsername,
			SentinelPassword: aclSentinelPassword,
		})

		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		sentinel = dicedb.NewSentinelClient(&dicedb.Options{
			Addr:       sentinelAddrs[0],
			MaxRetries: -1,
			Username:   aclSentinelUsername,
			Password:   aclSentinelPassword,
		})

		_, err := sentinel.GetMasterAddrByName(ctx, sentinelName).Result()
		Expect(err).NotTo(HaveOccurred())

		// Wait until sentinels are picked up by each other.
		for _, process := range sentinels() {
			Eventually(func() string {
				return process.Info(ctx).Val()
			}, "15s", "100ms").Should(ContainSubstring("sentinels=3"))
		}
	})

	AfterEach(func() {
		unauthCommand := dicedb.NewStatusCmd(ctx, "ACL", "DELUSER", aclSentinelUsername)

		for _, process := range sentinels() {
			err := process.Client.Process(ctx, unauthCommand)
			Expect(err).NotTo(HaveOccurred())
		}

		_ = client.Close()
		_ = sentinel.Close()
	})

	It("should still facilitate operations", func() {
		err := client.Set(ctx, "wow", "acl-auth", 0).Err()
		Expect(err).NotTo(HaveOccurred())

		val, err := client.Get(ctx, "wow").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("acl-auth"))
	})
})
