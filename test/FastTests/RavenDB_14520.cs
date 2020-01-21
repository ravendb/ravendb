using System;
using System.IO;
using Microsoft.AspNetCore.Server.Kestrel.Core.Internal.Infrastructure;
using Raven.Client.Util;
using Xunit;
using Xunit.Abstractions;

namespace FastTests
{
    public class RavenDB_14520 : NoDisposalNeeded
    {
        public RavenDB_14520(ITestOutputHelper output)
            : base(output)
        {
        }

        [Fact]
        public void ReadOnlyStreamShouldNotThrow()
        {
            var streamWithTimeout = new StreamWithTimeout(new MyReadOnlyStream());
            streamWithTimeout.ReadTimeout = 10;
        }

        private class MyReadOnlyStream : ReadOnlyStream
        {
            public override bool CanSeek => throw new NotImplementedException();

            public override long Length => throw new NotImplementedException();

            public override long Position { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

            public override void Flush()
            {
                throw new NotImplementedException();
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotImplementedException();
            }

            public override void SetLength(long value)
            {
                throw new NotImplementedException();
            }
        }
    }
}
