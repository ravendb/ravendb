using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Tests.Issues
{
    public class RavenDB_3691 : RavenNewTestBase
    {
        [Fact]
        public void CanPutDocumentWithMetadataPropertyBeingNull()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("test", null, new { }, new Dictionary<string, string> { { "Foo", null } });
                }
            }
        }
    }
}
