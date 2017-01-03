using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using FastTests.Server.Documents.Replication;
using Raven.Abstractions.Connection;
using Raven.Client.Document;
using Raven.Client.Replication.Messages;
using Raven.Json.Linq;
using Xunit;
using Raven.Client.Connection;

namespace FastTests.Server.Replication
{
    public class ReplicationTopologyDiscoveryTests : ReplicationTestsBase
    {
        [Fact]
        public async Task Without_replication_full_topology_should_return_empty_topology_info()
        {
            using (var store = GetDocumentStore())
            {
                var topologyInfo = await GetFullTopology(store);

                Assert.NotNull(topologyInfo); //sanity check
                Assert.Empty(topologyInfo.IncomingByIncomingDbId);
                Assert.Empty(topologyInfo.OutgoingByDbId);
                Assert.Empty(topologyInfo.OfflineByUrlAndDatabase);
            }
        }

        [Fact(Skip = "WIP")]
        public async Task Master_slave_topology_should_be_correctly_detected()
        {
            using (var master = GetDocumentStore())
            using (var slave = GetDocumentStore())
            {
                var masterDocumentDatabase = await GetDocumentDatabaseInstanceFor(master);
                var slaveDocumentDatabase = await GetDocumentDatabaseInstanceFor(slave);

                SetupReplication(master,slave);

                var topologyInfoFromMaster = await GetFullTopology(master);

                Assert.NotNull(topologyInfoFromMaster); //sanity check
                Assert.Empty(topologyInfoFromMaster.IncomingByIncomingDbId);                
                Assert.Empty(topologyInfoFromMaster.OfflineByUrlAndDatabase);

                Assert.Equal(1, topologyInfoFromMaster.OutgoingByDbId.Count);
                Assert.Equal(masterDocumentDatabase.DbId.ToString(), topologyInfoFromMaster.OutgoingByDbId.First().Key);
                Assert.Equal(slaveDocumentDatabase.DbId.ToString(), topologyInfoFromMaster.OutgoingByDbId.First().Value.DbId);
            }
        }

        private async Task<NodeTopologyInfo> GetFullTopology(DocumentStore store)
        {
            var url = $"{store.Url}/databases/{store.DefaultDatabase}/full-topology";
            using (var request = store.JsonRequestFactory.CreateHttpJsonRequest(
                new CreateHttpJsonRequestParams(null, url, 
                    HttpMethod.Get, 
                    new OperationCredentials(null, CredentialCache.DefaultCredentials), 
                    new DocumentConvention())))
            {
                request.ExecuteRequest();
                var responseJsonString = await request.Response.Content.ReadAsStringAsync();
                var topologyInfoJson = RavenJObject.Parse(responseJsonString);                
                return topologyInfoJson.ToObject<NodeTopologyInfo>();
            }

        }
    }
}
