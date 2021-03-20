using System;
using System.Threading;
using FastTests;
using Raven.Server.ServerWide;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6665 : NoDisposalNeeded
    {
        public RavenDB_6665(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WillThrowIfTimeoutIsInvalid()
        {
            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                using (new OperationCancelToken(TimeSpan.MinValue, CancellationToken.None, CancellationToken.None))
                {
                }
            });

            Assert.Throws<ArgumentOutOfRangeException>(() =>
            {
                using (new OperationCancelToken(TimeSpan.FromSeconds(-10), CancellationToken.None, CancellationToken.None))
                {
                }
            });
        }

        [Fact]
        public void WillNotThrowITimeoutIsValid()
        {
            using (new OperationCancelToken(TimeSpan.FromSeconds(10), CancellationToken.None, CancellationToken.None))
            {
            }

            using (new OperationCancelToken(TimeSpan.FromMilliseconds(-1), CancellationToken.None, CancellationToken.None)) // infinite
            {
            }
        }

        [Fact]
        public void WillTimeout()
        {
            using (var token = new OperationCancelToken(TimeSpan.FromSeconds(1), CancellationToken.None, CancellationToken.None))
            {
                Assert.False(token.Token.IsCancellationRequested);

                Thread.Sleep(TimeSpan.FromSeconds(2));

                Assert.True(token.Token.IsCancellationRequested);
            }
        }

        [Fact]
        public void WillThrowWhenDelayingInfiniteTimeout()
        {
            using (var token = new OperationCancelToken(CancellationToken.None, CancellationToken.None))
            {
                Assert.Throws<InvalidOperationException>(() => token.Delay());
            }

            using (var token = new OperationCancelToken(TimeSpan.FromMilliseconds(-1), CancellationToken.None, CancellationToken.None))
            {
                Assert.Throws<InvalidOperationException>(() => token.Delay());
            }
        }
    }
}
