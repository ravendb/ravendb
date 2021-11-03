using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Counters;
using Raven.Server.Utils;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17353 : ClusterTestBase
    {
        public RavenDB_17353(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CheckGetNodeByTag()
        {
            const string dbid1 = "07e2GrSMdkunq1AC+KgwIg";
            const string dbid2 = "F9I6Egqwm0Kz+K0oFVIR9Q";
            const string cv = "C:8397-07e2GrSMdkunq1AC+KgwIg, A:8917-3UiZOcXaZ0+d6GI/VTr//A, B:8397-5FYpkl5TX0SPlIBPwjmhUw, A:2568-F9I6Egqwm0Kz+K0oFVIR9Q, A:13366-IG4VwBTOnkqoT/uwgm2OQg, A:2568-OSKWIRBEDEGoAxbEIiFJeQ";

            var nodeTag = ChangeVectorUtils.GetNodeTagById(cv, dbid1);
            Assert.Equal("C", nodeTag);

            nodeTag = ChangeVectorUtils.GetNodeTagById(cv, dbid2);
            Assert.NotEqual(" A", nodeTag);
            Assert.Equal("A", nodeTag);
        }

        [Fact]
        public async Task GetFullCounterValues_NodeTagsShouldNotStartWithSpace()
        {
            var cluster = await CreateRaftCluster(3);
            var db = GetDatabaseName();
            await CreateDatabaseInCluster(db, 3, cluster.Leader.WebUrl);

            var stores = cluster.Nodes.Select(s => new DocumentStore
            {
                Database = db,
                Urls = new[] { s.WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
                .ToList();

            try
            {
                const string docId = "users/1";
                const string counter = "likes";

                using (var session = stores[0].OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User(), docId);
                    session.SaveChanges();
                }

                foreach (var store in stores)
                {
                    using (var session = store.OpenSession())
                    {
                        session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                        session.CountersFor(docId).Increment(counter);
                        session.SaveChanges();
                    }
                }

                foreach (var store in stores)
                {
                    var result = store.Operations.Send(new GetCountersOperation(docId, returnFullResults: true));
                    Assert.Equal(1, result.Counters.Count);
                    Assert.Equal(counter, result.Counters[0].CounterName);

                    var fullValues = result.Counters[0].CounterValues;
                    Assert.Equal(3, fullValues.Count);

                    var nodeTags = new HashSet<string> { "A", "B", "C" };
                    foreach (var kvp in fullValues)
                    {
                        Assert.False(kvp.Key.StartsWith(" "));

                        var tag = kvp.Key[0].ToString();
                        Assert.True(nodeTags.Contains(tag));
                        nodeTags.Remove(tag);
                    }
                }
            }
            finally
            {
                foreach (var item in stores)
                {
                    item.Dispose();
                }
            }
        }
    }
}
