using System.Collections.Generic;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Tests.Issues
{
    public class RavenDB_3691 : RavenTestBase
    {
        public RavenDB_3691(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
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
