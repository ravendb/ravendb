using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Corax;
using NetTopologySuite.Algorithm;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax
{
    public class TokenSpanTests : NoDisposalNeeded
    {
        public TokenSpanTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CreateTokenPosition()
        {
            TokenPosition position = new(10, 1);
            Assert.Equal(1, position.Offset);
            Assert.Equal(10, position.Storage);

            position.Offset = 5;
            Assert.Equal(5, position.Offset);
            Assert.Equal(10, position.Storage);
        }

        [Fact]
        public void CreateSpanPosition()
        {
            TokenSpan span = new(new(10, 1), 10);
            Assert.Equal(10, span.Length);
            Assert.Equal(1, span.Position.Offset);
            Assert.Equal(10, span.Position.Storage);
        }
    }
}
