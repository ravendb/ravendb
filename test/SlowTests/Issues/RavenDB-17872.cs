using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17872 : ClusterTestBase
    {
        public RavenDB_17872(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TestCase()
        {
            using var store = GetDocumentStore();

            const string id = "testObjs/0";
            const string metadataPropName = "RandomProp";
            const string metadataValue = "RandomValue";

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = new TestObj();
                session.Advanced.ClusterTransaction.CreateCompareExchangeValue(id, entity);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                // entity.Value.Prop = "Changed"; //without this line the session doesn't send an update to the compare exchange
                entity.Metadata[metadataPropName] = metadataValue;
                await session.SaveChangesAsync();
            }

            //The session doesn't set the metadata but it can be seen in the studio. 
            // WaitForUserToContinueTheTest(store);

            using (var session = store.OpenAsyncSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
            {
                var entity = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<TestObj>(id);
                Assert.Contains(metadataPropName, entity.Metadata.Keys);
                Assert.Equal(metadataValue, entity.Metadata[metadataPropName]);
            }
        }

        class TestObj
        {
            public string Id { get; set; }
            public string Prop { get; set; }
        }

    }
}
