using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Extensions.Primitives;
using Xunit;

namespace SlowTests.Tests.Issues
{
    public class RavenDB_3691 : RavenTestBase
    {
        [Fact]
        public void CanPutDocumentWithMetadataPropertyBeingNull()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("test", null, new { }, new Dictionary<string, StringValues> { { "Foo", (string)null } });
                }
            }
        }
    }
}
