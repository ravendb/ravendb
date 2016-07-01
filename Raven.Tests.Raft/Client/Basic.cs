// -----------------------------------------------------------------------
//  <copyright file="Basic.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Request;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Config;
using Raven.Database.Raft.Dto;
using Raven.Database.Raft.Util;
using Raven.Json.Linq;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Raft.Client
{
    public class Basic : RaftTestBase
    {
        protected override void ModifyConfiguration(InMemoryRavenConfiguration configuration)
        {
            configuration.Replication.ReplicationRequestTimeoutInMilliseconds = 4000;
        }

        [Fact]
        public void RequestExecuterShouldDependOnClusterBehavior()
        {
            using (var store = NewRemoteDocumentStore())
            {
                Assert.Equal(FailoverBehavior.AllowReadsFromSecondaries, store.Conventions.FailoverBehavior);

                var client = (ServerClient)store.DatabaseCommands;
                Assert.True(client.RequestExecuter is ReplicationAwareRequestExecuter);

                client = (ServerClient)store.DatabaseCommands.ForSystemDatabase();
                Assert.True(client.RequestExecuter is ReplicationAwareRequestExecuter);

                store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromLeaderWriteToLeader;
                client = (ServerClient)store.DatabaseCommands.ForDatabase(store.DefaultDatabase);
                Assert.True(client.RequestExecuter is ClusterAwareRequestExecuter);
            }
        }

        [Theory]
        [PropertyData("Nodes")]
        public void ClientsShouldBeAbleToPerformCommandsEvenIfTheyDoNotPointToLeader(int numberOfNodes)
        {
            var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store => store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromLeaderWriteToLeader);

            SetupClusterConfiguration(clusterStores);

            for (int i = 0; i < clusterStores.Count; i++)
            {
                var store = clusterStores[i];

                store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
            }

            for (int i = 0; i < clusterStores.Count; i++)
            {
                clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + i));
            }

            for (int i = 0; i < clusterStores.Count; i++)
            {
                var store = clusterStores[i];

                store.DatabaseCommands.Put("keys/" + (i + clusterStores.Count), null, new RavenJObject(), new RavenJObject());
            }

            for (int i = 0; i < clusterStores.Count; i++)
            {
                clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + (i + clusterStores.Count)));
            }
        }

        [Fact]
        public void NonClusterCommandsCanPerformCommandsOnClusterServers()
        {
            var clusterStores = CreateRaftCluster(2, activeBundles: "Replication", configureStore: store => store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromLeaderWriteToLeader);

            SetupClusterConfiguration(clusterStores);

            using (var store1 = clusterStores[0])
            using (var store2 = clusterStores[1])
            {
                using (ForceNonClusterRequests(new List<DocumentStore> {store1, store2}))
                {
                    var nonClusterCommands1 = (ServerClient)store1.DatabaseCommands;
                    var nonClusterCommands2 = (ServerClient)store2.DatabaseCommands;

                    nonClusterCommands1.Put("keys/1", null, new RavenJObject(), new RavenJObject());
                    nonClusterCommands2.Put("keys/2", null, new RavenJObject(), new RavenJObject());

                    var allNonClusterCommands = new[] { nonClusterCommands1, nonClusterCommands2 };

                    allNonClusterCommands.ForEach(commands => WaitForDocument(commands, "keys/1"));
                    allNonClusterCommands.ForEach(commands => WaitForDocument(commands, "keys/2"));
                }
            }
        }

        [Theory]
        [InlineData(3)]
        [InlineData(5)]
        public void ClientShouldHandleLeaderShutdown(int numberOfNodes)
        {
            using (WithCustomDatabaseSettings(doc => doc.Settings["Raven/Replication/ReplicationRequestTimeout"] = "4000"))
            {
                var clusterStores = CreateRaftCluster(numberOfNodes, activeBundles: "Replication", configureStore: store =>
                {
                    store.Conventions.FailoverBehavior = FailoverBehavior.ReadFromLeaderWriteToLeader;
                });

                foreach (var documentStore in clusterStores)
                {
                    // set lower timeout to reduce test time
                    documentStore.JsonRequestFactory.RequestTimeout = TimeSpan.FromSeconds(5);
                }

                SetupClusterConfiguration(clusterStores);

                clusterStores.ForEach(store => ((ServerClient)store.DatabaseCommands).RequestExecuter.UpdateReplicationInformationIfNeededAsync((AsyncServerClient)store.AsyncDatabaseCommands, force: true));

                for (int i = 0; i < clusterStores.Count; i++)
                {
                    var store = clusterStores[i];

                    store.DatabaseCommands.Put("keys/" + i, null, new RavenJObject(), new RavenJObject());
                }

                for (int i = 0; i < clusterStores.Count; i++)
                {
                    clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + i));
                }

                servers
                    .First(x => x.Options.ClusterManager.Value.IsLeader())
                    .Dispose();

                for (int i = 0; i < clusterStores.Count; i++)
                {
                    var store = clusterStores[i];
                    store.DatabaseCommands.Put("keys/" + (i + clusterStores.Count), null, new RavenJObject(), new RavenJObject());
                }

                for (int i = 0; i < clusterStores.Count; i++)
                {
                    clusterStores.ForEach(store => WaitForDocument(store.DatabaseCommands, "keys/" + (i + clusterStores.Count)));
                }
            }
        }

        [Fact]
        public async Task ClusterAwareControllersShouldRedirectToLeaderIfNotLeaderAndAllowActionsIfLeader()
        {
            var clusterStores = CreateRaftCluster(2);

            var nonLeaderServer = servers.Single(x => x.Options.ClusterManager.Value.IsLeader() == false);
            var leaderServer = servers.Single(x => x.Options.ClusterManager.Value.IsLeader());
            var nonLeaderStore = clusterStores[servers.IndexOf(nonLeaderServer)];
            var leaderStore = clusterStores[servers.IndexOf(leaderServer)];
            var nonLeaderUrl = nonLeaderStore.Url.ForDatabase(nonLeaderStore.DefaultDatabase);
            var leaderUrl = leaderStore.Url.ForDatabase(leaderStore.DefaultDatabase);

            var httpClient = new HttpClient(new HttpClientHandler { AllowAutoRedirect = false })
                                      {
                                          DefaultRequestHeaders =
                                          {
                                              { Constants.Cluster.ClusterAwareHeader, "true" }
                                          }
                                      };

            await AssertDocumentsController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertDocumentsController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertDocumentsBatchController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertDocumentsBatchController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertFacetsController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertFacetsController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertIdentityController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertIdentityController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertIndexController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertIndexController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertMoreLikeThisController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertMoreLikeThisController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertMultiGetController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertMultiGetController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertQueriesController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertQueriesController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertStaticController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertStaticController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertStreamsController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertStreamsController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertSuggestionController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertSuggestionController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertTermsController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertTermsController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));

            await AssertTransformersController(httpClient, nonLeaderUrl, response =>
            {
                Assert.False(response.IsSuccessStatusCode);
                Assert.Equal(HttpStatusCode.Redirect, response.StatusCode);
            });
            await AssertTransformersController(httpClient, leaderUrl, response => Assert.NotEqual(HttpStatusCode.Redirect, response.StatusCode));
        }

        private static async Task AssertTransformersController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.GetAsync(url + "/transformers/transformer1");
            assertion(response);

            response = await httpClient.GetAsync(url + "/transformers");
            assertion(response);

            response = await httpClient.PutAsync(url + "/transformers/transformer1", new JsonContent());
            assertion(response);

            response = await httpClient.DeleteAsync(url + "/transformers/transformer1");
            assertion(response);
        }

        private static async Task AssertTermsController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.GetAsync(url + "/terms/index1");
            assertion(response);
        }

        private static async Task AssertSuggestionController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.GetAsync(url + "/suggest/keys/1");
            assertion(response);
        }

        private static async Task AssertStreamsController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.GetAsync(url + "/streams/docs");
            assertion(response);

            response = await httpClient.GetAsync(url + "/streams/query/index1");
            assertion(response);

            response = await httpClient.PostAsync(url + "/streams/query/index1", new JsonContent());
            assertion(response);
        }

        private static async Task AssertStaticController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.GetAsync(url + "/static");
            assertion(response);

            response = await httpClient.GetAsync(url + "/static/attachment/1");
            assertion(response);

            response = await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Head, url + "/static/attachment/1"));
            assertion(response);

            response = await httpClient.PutAsync(url + "/static/attachment/1", new JsonContent());
            assertion(response);

            response = await httpClient.PostAsync(url + "/static/attachment/1", new JsonContent());
            assertion(response);

            response = await httpClient.DeleteAsync(url + "/static/attachment/1");
            assertion(response);
        }

        private static async Task AssertQueriesController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.GetAsync(url + "/queries");
            assertion(response);

            response = await httpClient.PostAsync(url + "/queries", new JsonContent());
            assertion(response);
        }

        private static async Task AssertMultiGetController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.PostAsync(url + "/multi_get", new JsonContent());
            assertion(response);
        }

        private static async Task AssertMoreLikeThisController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.GetAsync(url + "/morelikethis/keys/1");
            assertion(response);
        }

        private static async Task AssertIndexController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.GetAsync(url + "/indexes");
            assertion(response);

            response = await httpClient.GetAsync(url + "/indexes/index1");
            assertion(response);

            response = await httpClient.GetAsync(url + "/indexes/last-queried");
            assertion(response);

            response = await httpClient.PutAsync(url + "/indexes/index1", new JsonContent());
            assertion(response);

            response = await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Head, url + "/indexes/index1"));
            assertion(response);

            response = await httpClient.PostAsync(url + "/indexes/index1", new JsonContent());
            assertion(response);

            response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod("RESET"), url + "/indexes/index1"));
            assertion(response);

            response = await httpClient.DeleteAsync(url + "/indexes/index1");
            assertion(response);

            response = await httpClient.PostAsync(url + "/indexes/set-priority/index1", new JsonContent());
            assertion(response);

            response = await httpClient.GetAsync(url + "/c-sharp-index-definition/index1");
            assertion(response);
        }

        private static async Task AssertIdentityController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.PostAsync(url + "/identity/next", new JsonContent());
            assertion(response);

            response = await httpClient.PostAsync(url + "/identity/seed", new JsonContent());
            assertion(response);
        }

        private static async Task AssertFacetsController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.GetAsync(url + "/facets/facet1");
            assertion(response);

            response = await httpClient.PostAsync(url + "/facets/facet1", new JsonContent());
            assertion(response);

            response = await httpClient.PostAsync(url + "/facets/multisearch", new JsonContent());
            assertion(response);
        }

        private static async Task AssertDocumentsBatchController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.PostAsync(url + "/bulk_docs", new JsonContent());
            assertion(response);
        }

        private static async Task AssertDocumentsController(HttpClient httpClient, string url, Action<HttpResponseMessage> assertion)
        {
            var response = await httpClient.GetAsync(url.Docs(0, 1024));
            assertion(response);

            response = await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Head, url + "/docs?id=keys/1"));
            assertion(response);

            response = await httpClient.PutAsync(url.Doc("keys/1"), new JsonContent());
            assertion(response);

            response = await httpClient.PostAsync(url+ "/docs", new JsonContent());
            assertion(response);

            response = await httpClient.SendAsync(new HttpRequestMessage(HttpMethod.Head, url.Doc("keys/1")));
            assertion(response);

            response = await httpClient.GetAsync(url.Doc("keys/1"));
            assertion(response);

            response = await httpClient.DeleteAsync(url.Doc("keys/1"));
            assertion(response);

            response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod("PATCH"), url.Doc("keys/1")) { Content = new JsonContent(new RavenJObject()) });
            assertion(response);

            response = await httpClient.SendAsync(new HttpRequestMessage(new HttpMethod("EVAL"), url.Doc("keys/1")));
            assertion(response);
        }
    }
}
