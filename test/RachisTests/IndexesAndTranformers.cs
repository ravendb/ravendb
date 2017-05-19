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

        public async Task<Tuple<long,List<RavenServer>>> CreateDatabaseInCluster(string databaseName, int replicationFactor, string leasderUrl)
        {
            CreateDatabaseResult databaseResult;
            using (var store = new DocumentStore()
            {
                Url = leasderUrl,
                Database = databaseName
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
            if(numberOfInstances != replicationFactor)
                throw new InvalidOperationException("Couldn't create the db on all nodes, just on " + numberOfInstances + " out of " + replicationFactor);
            return Tuple.Create(databaseResult.ETag.Value,
                Servers.Where(s => databaseResult.Topology.RelevantFor(s.ServerStore.NodeTag)).ToList());
            
            
        }

        [Fact]
        public async Task BasicTransformerCreation()
        {
            var leader = await this.CreateRaftClusterAndGetLeader(5);

            var defaultDatabase = "BasicTransformerCreation";
            var databaseCreationResult = await CreateDatabaseInCluster(defaultDatabase, 5, leader.WebUrls[0]);
            using (var store = new DocumentStore()
            {
                Url =  databaseCreationResult.Item2[0].WebUrls[0],
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
                        Url = serverToStoreAt.WebUrls[0],
                        Database = store.Database
                    })
                    {
                        var putTransformerResult = await serverToStoreAtStore.Admin.SendAsync(new PutTransformerOperation(curTransformerDefinition),
                            CancellationToken.None);

                        foreach (var serverToCheckAt in relevantServers)
                        {
                            var db = await serverToCheckAt.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);

                            Assert.True(await db.WaitForIndexNotification(putTransformerResult.Etag).WaitAsync(WaitInterval));
                            using (var currentServerStore = new DocumentStore
                            {
                                Url = serverToCheckAt.WebUrls[0],
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