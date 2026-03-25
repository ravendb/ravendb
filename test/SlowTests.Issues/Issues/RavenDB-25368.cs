using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25368 : RavenTestBase
    {
        public RavenDB_25368(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Revisions | RavenTestCategory.ClientApi)]
        public async Task LoadedRevisionsBecomingBatchCommandsAndChangesTheDoc()
        {
            const string id = "testObjs/1-A";

            using var store = GetDocumentStore();

            await RevisionsHelper.SetupRevisionsAsync(store, configuration: new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration()
            });

            using (var session = store.OpenAsyncSession())
            {
                var testObj = new TestObjV1 { Version = "v1" };
                await session.StoreAsync(testObj, id);
                await session.SaveChangesAsync();

                testObj.Version = "v2";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var obj = await session.LoadAsync<TestObjV2>(id);
                await session.Advanced.Revisions.GetForAsync<TestObjV2>(obj.Id);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var testObj = await session.LoadAsync<TestObjV1>(id);
                var version = testObj.Version;
                Assert.NotNull(version);
                Assert.NotEmpty(version);
                Assert.Equal("v2", version);
            }
        }

        public class TestObjV1
        {
            public string Id { get; set; }
            public string Version { get; set; }
        }

        public class TestObjV2
        {
            public string Id { get; set; }

            public DateTime CreatedDate { get; set; }
            public string CreatedByUserName { get; set; }
        }
    }
}
