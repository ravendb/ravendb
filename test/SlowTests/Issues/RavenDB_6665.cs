using System;
using System.Threading;
using FastTests;
using Raven.Server.ServerWide;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6665 : NoDisposalNeeded
    {
        [Fact]
        public void WillThrowIfTimeoutIsInvalid()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                using (new OperationCancelToken(TimeSpan.MinValue, CancellationToken.None))
                {
                }
            });

            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                using (new OperationCancelToken(TimeSpan.FromSeconds(-10), CancellationToken.None))
                {
                }
            });
        }

        [Fact]
        public void WillNotThrowITimeoutIsValid()
        {
            using (new OperationCancelToken(TimeSpan.FromSeconds(10), CancellationToken.None))
            {
            }

            using (new OperationCancelToken(TimeSpan.FromMilliseconds(-1), CancellationToken.None)) // infinite
            {
            }
        }

        [Fact]
        public void WillTimeout()
        {
            using (var token = new OperationCancelToken(TimeSpan.FromSeconds(1), CancellationToken.None))
            {
                Assert.False(token.Token.IsCancellationRequested);

                Thread.Sleep(1100);

                Assert.True(token.Token.IsCancellationRequested);
            }
        }

        [Fact]
        public void CanDelayTimeout()
        {
            using (var token = new OperationCancelToken(TimeSpan.FromSeconds(1), CancellationToken.None))
            {
                Assert.False(token.Token.IsCancellationRequested);

                Thread.Sleep(500);

                Assert.False(token.Token.IsCancellationRequested);

                token.Delay();

                Thread.Sleep(700);

                Assert.False(token.Token.IsCancellationRequested);

                Thread.Sleep(400);

                Assert.True(token.Token.IsCancellationRequested);
            }
        }

        [Fact]
        public void WillThrowWhenDelayingInfiniteTimeout()
        {
            using (var token = new OperationCancelToken(CancellationToken.None))
            {
                Assert.Throws<InvalidOperationException>(() => token.Delay());
            }

            using (var token = new OperationCancelToken(TimeSpan.FromMilliseconds(-1), CancellationToken.None))
            {
                Assert.Throws<InvalidOperationException>(() => token.Delay());
            }
        }
    }
}