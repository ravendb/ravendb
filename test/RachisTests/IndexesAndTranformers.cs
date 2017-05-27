using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Transformers;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Rachis
{
    public class IndexesAndTransformers : ClusterTestBase
    {
        private class Person
        {
            public string Name { get; set; }
        }

        private class Transformer : AbstractTransformerCreationTask<Person>
        {
            public Transformer()
            {
                TransformResults = docs => from doc in docs
                    select new {Nmae = doc.Name};
            }
        }

        public static TimeSpan WaitInterval = TimeSpan.FromSeconds(5);

        [Fact]
        public async Task BasicTransformerCreation()
        {
            var leader = await this.CreateRaftClusterAndGetLeader(5);

            var defaultDatabase = "BasicTransformerCreation";
            var databaseCreationResult = await CreateDatabaseInCluster(defaultDatabase, 5, leader.WebUrls[0]);
            using (var store = new DocumentStore()
            {
                Urls =  databaseCreationResult.Item2[0].WebUrls,
                Database = defaultDatabase
            }.Initialize())
            {
                var relevantServers = databaseCreationResult.Item2;
                for (var i = 0; i < relevantServers.Count; i++)
                {
                    var serverToStoreAt = relevantServers[i];
                    LambdaExpression tranformResults = (Expression<Func<IEnumerable<Person>, IEnumerable>>)(docs => from doc in docs
                        select new {doc.Name});
                    var curTransformerDefinition = new TransformerDefinition
                    {
                        Name = "Trans" + i,
                        TransformResults = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Person, object>(
                            tranformResults, DocumentConventions.Default, "results", translateIdentityProperty: false),
                    };

                    using (var serverToStoreAtStore = new DocumentStore
                    {
                        Urls = serverToStoreAt.WebUrls,
                        Database = store.Database
                    })
                    {
                        var putTransformerResult = await serverToStoreAtStore.Admin.SendAsync(new PutTransformerOperation(curTransformerDefinition),
                            CancellationToken.None);

                        foreach (var serverToCheckAt in relevantServers)
                        {
                            var db = await serverToCheckAt.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                            Assert.True(await db.RachisLogIndexNotifications.WaitForIndexNotification(putTransformerResult.Etag).WaitAsync(WaitInterval));
                            using (var currentServerStore = new DocumentStore
                            {
                                Urls = serverToStoreAt.WebUrls,
                                Database = store.Database
                            })
                            {
                                var transformerDefinition = db.TransformerStore.GetTransformer(curTransformerDefinition.Name);
                                Assert.Equal(curTransformerDefinition.Name, transformerDefinition.Name);
                            }
                        }
                    }
                }
            }
        }
    }
}