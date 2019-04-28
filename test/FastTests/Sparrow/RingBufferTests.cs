using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Collections;
using Sparrow.Threading;
using Xunit;

namespace FastTests.Sparrow
{
    public class RingBufferTests : NoDisposalNeeded
    {
        [Fact]
        public void RingBuffer_SingleItemPush()
        {
            var rb = new SingleConsumerRingBuffer<long>(3);

            long item = 910;
            Assert.True(rb.TryPush(ref item));
            Assert.True(rb.TryPush(ref item));
            Assert.True(rb.TryPush(ref item));
            Assert.True(rb.TryPush(ref item));
            Assert.False(rb.TryPush(ref item));

            var span = rb.Acquire();
            Assert.Equal(4, span.Length);
            Assert.Equal(910, span[0].Item);
            Assert.Equal(910, span[1].Item);
            Assert.Equal(910, span[2].Item);
            Assert.Equal(910, span[3].Item);

            item = 911;
            Assert.False(rb.TryPush(ref item));
            rb.Release();

            Assert.True(rb.TryPush(ref item));

            span = rb.Acquire();
            Assert.Equal(1, span.Length);
            Assert.Equal(911, span[0].Item);
            rb.Release();
        }

        [Fact]
        public void RingBuffer_MultipleItemsPush()
        {
            var rb = new SingleConsumerRingBuffer<long>(4);

            long item = 910;
            Assert.True(rb.TryPush(ref item));
            Assert.True(rb.TryPush(ref item));

            var span = rb.Acquire();
            Assert.Equal(2, span.Length);
            Assert.Equal(910, span[0].Item);
            Assert.Equal(910, span[1].Item);

            item = 911;
            Assert.True(rb.TryPush(ref item));
            Assert.True(rb.TryPush(ref item));
            Assert.False(rb.TryPush(ref item));

            rb.Release();

            item = 912;
            Assert.True(rb.TryPush(ref item));

            span = rb.Acquire();
            Assert.Equal(2, span.Length);
            Assert.Equal(911, span[0].Item);
            Assert.Equal(911, span[1].Item);
            rb.Release();

            span = rb.Acquire();
            Assert.Equal(1, span.Length);
            Assert.Equal(912, span[0].Item);
            rb.Release();
        }

        [Fact]
        public void RingBuffer_MultipleItemsSingleAcquirePush()
        {
            var rb = new SingleConsumerRingBuffer<long>(4);

            long item = 910;
            Assert.True(rb.TryPush(ref item));
            item = 911;
            Assert.True(rb.TryPush(ref item));

            RingItem<long> ringItem;
            Assert.True(rb.TryAcquireSingle(out ringItem));
            Assert.Equal(910, ringItem.Item);
            Assert.True(rb.TryAcquireSingle(out ringItem));
            Assert.Equal(911, ringItem.Item);

            item = 912;
            Assert.True(rb.TryPush(ref item));
            Assert.True(rb.TryPush(ref item));
            Assert.False(rb.TryPush(ref item));

            rb.Release();

            item = 913;
            Assert.True(rb.TryPush(ref item));

            Assert.True(rb.TryAcquireSingle(out ringItem));
            Assert.Equal(912, ringItem.Item);
            Assert.True(rb.TryAcquireSingle(out ringItem));
            Assert.Equal(912, ringItem.Item);
            rb.Release();

            Assert.True(rb.TryAcquireSingle(out ringItem));
            Assert.Equal(913, ringItem.Item);
            rb.Release();
        }

        [Fact]
        public void RingBuffer_MultipleAcquires_SingleRelease()
        {
            var rb = new SingleConsumerRingBuffer<long>(4);

            long item = 910;
            Assert.True(rb.TryPush(ref item));
            Assert.True(rb.TryPush(ref item));

            var span = rb.Acquire();
            Assert.Equal(2, span.Length);
            Assert.Equal(910, span[0].Item);
            Assert.Equal(910, span[1].Item);

            item = 911;
            Assert.True(rb.TryPush(ref item));
            Assert.True(rb.TryPush(ref item));

            span = rb.Acquire();
            Assert.Equal(2, span.Length);
            Assert.Equal(911, span[0].Item);
            Assert.Equal(911, span[1].Item);

            item = 912;
            Assert.False(rb.TryPush(ref item));

            rb.Release();

            item = 912;
            Assert.True(rb.TryPush(ref item));
        }

        [Fact]
        public void RingBuffer_WhenEmptyAndSimultaneouslyTryPushAndTryAcquired_ShouldAcquiredLegal()
        {
            var rb = new SingleConsumerRingBuffer<TestItem>(4);

            var acquiredEnd = new SingleUseFlag();
            var finished = new Barrier(2);
            var cancellation = new CancellationTokenSource();
            var timeout = TimeSpan.FromSeconds(1);

            var exceptions = new List<Exception>();
            var pushTask = Task.Run(() =>
            {
                try
                {
                    while (acquiredEnd.IsRaised() == false)
                    {
                        if (finished.SignalAndWait(timeout, cancellation.Token) == false)
                            break;

                        var testItem = new TestItem
                        {
                            TestProperty = finished.CurrentPhaseNumber
                        };
                        rb.TryPush(ref testItem);
                    }
                }
                catch (OperationCanceledException)
                {

                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
            }, cancellation.Token);

            try
            {
                for (var i = 0; i < 1000; i++)
                {
                    if (finished.SignalAndWait(timeout) == false)
                        break;

                    for (int j = 0; j < 100; j++)
                    {
                        if (rb.TryAcquireSingle(out var ringItem))
                        {
                            Assert.NotNull(ringItem.Item);
                            Assert.True(ringItem.IsReady == 1, $"Acquired {ringItem.GetType()} should be set to ready");
                            break;
                        }
                    }
                }
            }
            catch (Exception e)
            {
                exceptions.Add(e);
            }
            finally
            {
                cancellation.Cancel();
                acquiredEnd.Raise();
                try
                {
                    pushTask.Wait(timeout);
                }
                catch (Exception e)
                {
                    exceptions.Add(e);
                }
                cancellation.Dispose();

                if(exceptions.Any())
                    throw new AggregateException(exceptions);
            }
        }
        private class TestItem
        {
            public long TestProperty { get; set; }
        }
    }
}
