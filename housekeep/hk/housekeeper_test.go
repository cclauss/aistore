// Package hk provides mechanism for registering cleanup
// functions which are invoked at specified intervals.
/*
 * Copyright (c) 2018-2020, NVIDIA CORPORATION. All rights reserved.
 */
package hk

import (
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Housekeeper", func() {
	BeforeEach(func() {
		initCleaner()
	})

	It("should register the callback and fire it", func() {
		fired := false
		Housekeeper.Register("", func() time.Duration {
			fired = true
			return time.Second
		})

		time.Sleep(20 * time.Millisecond)
		Expect(fired).To(BeTrue()) // callback should be fired at the start
		fired = false

		time.Sleep(500 * time.Millisecond)
		Expect(fired).To(BeFalse())

		time.Sleep(600 * time.Millisecond)
		Expect(fired).To(BeTrue())
	})

	It("should register the callback and fire it after initial interval", func() {
		fired := false
		Housekeeper.Register("", func() time.Duration {
			fired = true
			return time.Second
		}, time.Second)

		time.Sleep(500 * time.Millisecond)
		Expect(fired).To(BeFalse())

		time.Sleep(600 * time.Millisecond)
		Expect(fired).To(BeTrue())
	})

	It("should register multiple callbacks and fire it in correct order", func() {
		fired := make([]bool, 2)
		Housekeeper.Register("foo", func() time.Duration {
			fired[0] = true
			return 2 * time.Second
		})
		Housekeeper.Register("bar", func() time.Duration {
			fired[1] = true
			return time.Second + 500*time.Millisecond
		})

		time.Sleep(20 * time.Millisecond)
		// "foo" and "bar" should fire at the start (no initial interval)
		for idx := 0; idx < len(fired); idx++ {
			Expect(fired[idx]).To(BeTrue())
			fired[idx] = false
		}

		time.Sleep(600 * time.Millisecond) // ~600ms

		// "foo" nor "bar" should fire
		Expect(fired[0] || fired[1]).To(BeFalse())

		time.Sleep(time.Second) // ~1.6s

		// "bar" should fire
		Expect(fired[0]).To(BeFalse())
		Expect(fired[1]).To(BeTrue())
		fired[1] = false

		time.Sleep(500 * time.Millisecond) // ~2.1s

		// "foo" should fire
		Expect(fired[0]).To(BeTrue())
		Expect(fired[1]).To(BeFalse())

		time.Sleep(time.Second) // ~3.1s

		// "bar" should fire once again
		Expect(fired[0] && fired[1]).To(BeTrue())
	})

	It("should unregister callback", func() {
		fired := make([]bool, 2)
		Housekeeper.Register("bar", func() time.Duration {
			fired[0] = true
			return 400 * time.Millisecond
		}, 400*time.Millisecond)
		Housekeeper.Register("foo", func() time.Duration {
			fired[1] = true
			return 200 * time.Millisecond
		}, 200*time.Millisecond)

		time.Sleep(500 * time.Millisecond)
		Expect(fired[0] && fired[1]).To(BeTrue()) // after ~2sec both callback should fire

		fired[0] = false
		fired[1] = false
		Housekeeper.Unregister("foo")

		time.Sleep(time.Second)
		Expect(fired[1]).To(BeFalse())
		Expect(fired[0]).To(BeTrue())

		Housekeeper.Unregister("bar")
	})

	It("should register and unregister multiple callbacks", func() {
		var fired bool
		f := func(name string) {
			Expect(fired).To(BeFalse())
			Housekeeper.Register(name, func() time.Duration {
				fired = true
				return 100 * time.Millisecond
			}, 100*time.Millisecond)

			time.Sleep(110 * time.Millisecond)
			Expect(fired).To(BeTrue())

			Housekeeper.Unregister(name)
			fired = false
		}

		f("foo")
		f("bar")
		f("baz")

		time.Sleep(time.Second)
		Expect(fired).To(BeFalse())
	})
})
