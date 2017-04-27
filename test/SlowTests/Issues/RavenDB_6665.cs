using System;
using System.Threading;
using System.Threading.Tasks;
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
        public async Task WillTimeout()
        {
            using (var token = new OperationCancelToken(TimeSpan.FromSeconds(1), CancellationToken.None))
            {
                Assert.False(token.Token.IsCancellationRequested);

                await Task.Delay(TimeSpan.FromSeconds(2));

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