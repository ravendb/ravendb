using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Transformers;
using Raven.Client.Documents.Transformers;
using Raven.Client.Server;
using Raven.Client.Server.Operations;
using Raven.Server;
using SlowTests.Bugs;
using SlowTests.MailingList;
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

        public async Task<Tuple<long,List<RavenServer>>> CreateDatabaseInCluster(string databaseName, int replicationFactor, string leasderUrl)
        {
            CreateDatabaseResult databaseResult;
            using (var store = new DocumentStore()
            {
                Url = leasderUrl,
                DefaultDatabase = databaseName
            }.Initialize())
            {
                var doc = MultiDatabase.CreateDatabaseDocument(databaseName);
                databaseResult = store.Admin.Server.Send(new CreateDatabaseOperation(doc, replicationFactor));
            }
            int numberOfInstances = 0;
            foreach (var server in Servers.Where(s => databaseResult.Topology.RelevantFor(s.ServerStore.NodeTag)))
            {
                await server.ServerStore.Cluster.WaitForIndexNotification(databaseResult.ETag ?? 0);
                try
                {
                    await server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(databaseName);
                    numberOfInstances++;
                }
                catch
                {

                }

            }
            return Tuple.Create(databaseResult.ETag.Value,
                Servers.Where(s => databaseResult.Topology.RelevantFor(s.ServerStore.NodeTag)).ToList());
            
            
        }

        [Fact]
        public async Task BasicTransformerCreation()
        {
            var leader = await this.CreateRaftClusterAndGetLeader(5);

            var defaultDatabase = "BasicTransformerCreation";
            var databaseCreationResult = await CreateDatabaseInCluster(defaultDatabase, 5, leader.WebUrls[0]);
            var index = databaseCreationResult.Item1;
            using (var store = new DocumentStore()
            {
                Url =  databaseCreationResult.Item2[0].WebUrls[0],
                DefaultDatabase = defaultDatabase
            }.Initialize())
            {
                for (var i = 0; i < databaseCreationResult.Item2.Count; i++)
                {
                    var serverToStoreAt = databaseCreationResult.Item2[i];
                    LambdaExpression tranformResults = (Expression<Func<IEnumerable<Person>, IEnumerable>>)(docs => from doc in docs
                        select new {Nmae = doc.Name});
                    var curTransformerDefinition = new TransformerDefinition
                    {
                        Name = "Trans" + i,
                        TransformResults = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Person, object>(
                            tranformResults, DocumentConventions.Default, "results", translateIdentityProperty: false),
                    };

                    using (var serverToStoreAtStore = new DocumentStore
                    {
                        Url = serverToStoreAt.WebUrls[0],
                        DefaultDatabase = store.DefaultDatabase
                    })
                    {
                        var putTransformerResult = await serverToStoreAtStore.Admin.SendAsync(new PutTransformerOperation(curTransformerDefinition),
                            CancellationToken.None);

                        foreach (var serverToCheckAt in this.Servers)
                        {
                            Assert.True(serverToCheckAt.ServerStore.Cluster.WaitForIndexNotification(putTransformerResult.Etag).Wait(WaitInterval));
                            using (var currentServerStore = new DocumentStore
                            {
                                Url = serverToCheckAt.WebUrls[0],
                                DefaultDatabase = store.DefaultDatabase
                            })
                            {
                                var getTransformerOperation = new GetTransformerOperation(curTransformerDefinition.Name);
                                var transformerDefinition = await currentServerStore.Admin.SendAsync(getTransformerOperation);
                                Assert.Equal(curTransformerDefinition.Name, transformerDefinition.Name);
                            }
                        }
                    }
                }
            }
            await Task.Delay(1000); //remove this
        }
    }
}