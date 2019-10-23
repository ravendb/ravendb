using System.Collections.Generic;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Issues
{
    public class RavenDB_3691 : RavenTestBase
    {
        public RavenDB_3691(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanPutDocumentWithMetadataPropertyBeingNull()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("test", null, new { }, new Dictionary<string, object> { { "Foo", (string)null } });
                }
            }
        }
    }
}
