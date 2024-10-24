/*
Copyright 2022 The Volcano Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package jobseq

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	watchtools "k8s.io/client-go/tools/watch"

	"volcano.sh/apis/pkg/apis/scheduling/v1beta1"
	e2eutil "volcano.sh/volcano/test/e2e/util"
)

var _ = ginkgo.Describe("Queue Job Status Transition", func() {

	var ctx *e2eutil.TestContext
	ginkgo.AfterEach(func() {
		e2eutil.CleanupTestContext(ctx)
	})

	ginkgo.It("Transform from inqueque to running should succeed", func() {
		ginkgo.By("Prepare 2 job")
		var q1 string
		var rep int32
		q1 = "queue-jobs-status-transition"
		ctx = e2eutil.InitTestContext(e2eutil.Options{
			Queues: []string{q1},
		})
		slot := e2eutil.HalfCPU
		rep = e2eutil.ClusterSize(ctx, slot)

		if rep < 4 {
			err := fmt.Errorf("You need at least 2 logical cpu for this test case, please skip 'Queue Job Status Transition' when you see this message")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for i := 0; i < 2; i++ {
			spec := &e2eutil.JobSpec{
				Tasks: []e2eutil.TaskSpec{
					{
						Name: "queue-job",
						Img:  e2eutil.DefaultNginxImage,
						Req:  slot,
						Min:  rep,
						Rep:  rep,
					},
				},
			}
			spec.Name = "queue-job-status-transition-test-job-" + strconv.Itoa(i)
			spec.Queue = q1
			e2eutil.CreateJob(ctx, spec)
		}

		ginkgo.By("Verify queue have pod groups inqueue")
		err := e2eutil.WaitQueueStatus(func(_ context.Context) (bool, error) {
			queue, err := ctx.Vcclient.SchedulingV1beta1().Queues().Get(context.TODO(), q1, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Get queue %s failed", q1)
			return queue.Status.Inqueue > 0, nil
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error waiting for queue inqueue")

		ginkgo.By("Verify queue have pod groups running")
		err = e2eutil.WaitQueueStatus(func(_ context.Context) (bool, error) {
			queue, err := ctx.Vcclient.SchedulingV1beta1().Queues().Get(context.TODO(), q1, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Get queue %s failed", q1)
			return queue.Status.Running > 0, nil
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error waiting for queue running")
	})

	ginkgo.It("Transform from running to pending should succeed", func() {
		ginkgo.By("Prepare 2 job")
		var q1 string
		var podNamespace string
		var rep int32
		var firstJobName string

		q1 = "queue-jobs-status-transition"
		ctx = e2eutil.InitTestContext(e2eutil.Options{
			Queues: []string{q1},
		})
		podNamespace = ctx.Namespace
		slot := e2eutil.HalfCPU
		rep = e2eutil.ClusterSize(ctx, slot)

		if rep < 4 {
			err := fmt.Errorf("You need at least 2 logical cpu for this test case, please skip 'Queue Job Status Transition' when you see this message")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for i := 0; i < 2; i++ {
			spec := &e2eutil.JobSpec{
				Tasks: []e2eutil.TaskSpec{
					{
						Name: "queue-job",
						Img:  e2eutil.DefaultNginxImage,
						Req:  slot,
						Min:  rep,
						Rep:  rep,
					},
				},
			}
			spec.Name = "queue-job-status-transition-test-job-" + strconv.Itoa(i)
			if i == 0 {
				firstJobName = spec.Name
			}
			spec.Queue = q1
			e2eutil.CreateJob(ctx, spec)
		}

		ginkgo.By("Verify queue have pod groups running")
		err := e2eutil.WaitQueueStatus(func(_ context.Context) (bool, error) {
			queue, err := ctx.Vcclient.SchedulingV1beta1().Queues().Get(context.TODO(), q1, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Get queue %s failed", q1)
			return queue.Status.Running > 0, nil
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error waiting for queue running")

		clusterPods, err := ctx.Kubeclient.CoreV1().Pods(podNamespace).List(context.TODO(), metav1.ListOptions{})
		for _, pod := range clusterPods.Items {
			if pod.Labels["volcano.sh/job-name"] == firstJobName {
				err = ctx.Kubeclient.CoreV1().Pods(podNamespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to delete pod %s", pod.Name)
			}
		}

		ginkgo.By("Verify queue have pod groups Pending")
		err = e2eutil.WaitQueueStatus(func(_ context.Context) (bool, error) {
			queue, err := ctx.Vcclient.SchedulingV1beta1().Queues().Get(context.TODO(), q1, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Get queue %s failed", q1)
			return queue.Status.Pending > 0, nil
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error waiting for queue Pending")
	})

	ginkgo.It("Transform from running to unknown should succeed", func() {
		ginkgo.By("Prepare 2 job")
		var q1 string
		var podNamespace string
		var rep int32

		q1 = "queue-jobs-status-transition"
		ctx = e2eutil.InitTestContext(e2eutil.Options{
			Queues: []string{q1},
		})
		podNamespace = ctx.Namespace
		slot := e2eutil.HalfCPU
		rep = e2eutil.ClusterSize(ctx, slot)

		if rep < 4 {
			err := fmt.Errorf("You need at least 2 logical cpu for this test case, please skip 'Queue Job Status Transition' when you see this message")
			gomega.Expect(err).NotTo(gomega.HaveOccurred())
		}

		for i := 0; i < 2; i++ {
			spec := &e2eutil.JobSpec{
				Tasks: []e2eutil.TaskSpec{
					{
						Name: "queue-job",
						Img:  e2eutil.DefaultNginxImage,
						Req:  slot,
						Min:  rep,
						Rep:  rep,
					},
				},
			}
			spec.Name = "queue-job-status-transition-test-job-" + strconv.Itoa(i)
			spec.Queue = q1
			e2eutil.CreateJob(ctx, spec)
		}

		ginkgo.By("Verify queue have pod groups running")
		err := e2eutil.WaitQueueStatus(func(_ context.Context) (bool, error) {
			queue, err := ctx.Vcclient.SchedulingV1beta1().Queues().Get(context.TODO(), q1, metav1.GetOptions{})
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Get queue %s failed", q1)
			return queue.Status.Running > 0, nil
		})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error waiting for queue running")

		ginkgo.By("Delete some of pod which will case pod group status transform from running to unknown.")
		podDeleteNum := 0

		err = e2eutil.WaitPodPhaseRunningMoreThanNum(ctx, podNamespace, 2)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed waiting for pods")

		clusterPods, err := ctx.Kubeclient.CoreV1().Pods(podNamespace).List(context.TODO(), metav1.ListOptions{})
		for _, pod := range clusterPods.Items {
			if pod.Status.Phase == corev1.PodRunning {
				err = ctx.Kubeclient.CoreV1().Pods(podNamespace).Delete(context.TODO(), pod.Name, metav1.DeleteOptions{})
				gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Failed to delete pod %s", pod.Name)
				podDeleteNum = podDeleteNum + 1
			}
			if podDeleteNum >= int(rep/2) {
				break
			}
		}

		ginkgo.By("Verify queue have pod groups unknown")
		fieldSelector := fields.OneTermEqualSelector("metadata.name", q1).String()
		w := &cache.ListWatch{
			WatchFunc: func(options metav1.ListOptions) (i watch.Interface, e error) {
				options.FieldSelector = fieldSelector
				return ctx.Vcclient.SchedulingV1beta1().Queues().Watch(context.TODO(), options)
			},
		}
		wctx, cancel := watchtools.ContextWithOptionalTimeout(context.Background(), 5*time.Minute)
		defer cancel()

		_, err = watchtools.Until(wctx, clusterPods.ResourceVersion, w, func(event watch.Event) (bool, error) {
			switch t := event.Object.(type) {
			case *v1beta1.Queue:
				if t.Status.Unknown > 0 {
					return true, nil
				}
			}
			return false, nil
		})

		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "Error waiting for queue unknown")
	})
})
