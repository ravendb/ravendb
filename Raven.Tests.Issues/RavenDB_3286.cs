// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3286.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;

using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Metrics;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3286 : ReplicationBase
    {
        [Fact]
        public void RequestTimeMetricRequiresLowerThresholdToSwitchBackIfItHasSurpassed()
        {
            var conventions = new DocumentConvention
                              {
                                  RequestTimeThresholdInMilliseconds = 100 // switch back threshold will be 75
                              };

            var metric = new RequestTimeMetric();

            Assert.False(metric.RateSurpassed(conventions));

            UpdateMetric(metric, 500, 60); // 500

            Assert.True(metric.RateSurpassed(conventions));

            UpdateMetric(metric, 15, 24); // 75+

            Assert.True(metric.RateSurpassed(conventions));

            UpdateMetric(metric, 15, 1); // 70

            Assert.False(metric.RateSurpassed(conventions));
        }

        [Fact]
        public void DecreasingTimeMetricDecreasedRequestTimeMetricAfterEachRequest()
        {
            var conventions = new DocumentConvention
            {
                RequestTimeThresholdInMilliseconds = 100 // switch back threshold will be 75
            };

            var metric = new RequestTimeMetric();

            UpdateMetric(metric, 500, 60); // 500

            Assert.True(metric.RateSurpassed(conventions));

            var decreasingMetric = new DecreasingTimeMetric(metric);

            UpdateMetric(decreasingMetric, 500, 30); // 77+

            Assert.True(metric.RateSurpassed(conventions));

            UpdateMetric(decreasingMetric, 500, 1); // 73

            Assert.False(metric.RateSurpassed(conventions));
        }

        [Fact]
        public async Task Basic()
        {
            using (var store1 = CreateStore(configureStore: s => s.Conventions.FailoverBehavior = FailoverBehavior.AllowReadFromSecondariesWhenRequestTimeThresholdIsSurpassed))
            using (var store2 = CreateStore())
            {
                SetupReplication(store1.DatabaseCommands, store2);

                var replicationInformer = store1.GetReplicationInformerForDatabase();
                replicationInformer.ClearReplicationInformationLocalCache((ServerClient)store1.DatabaseCommands);
                replicationInformer.RefreshReplicationInformation((ServerClient)store1.DatabaseCommands);

                await PauseReplicationAsync(0, store1.DefaultDatabase);
                await PauseReplicationAsync(1, store2.DefaultDatabase);

                servers.ForEach(server => server.Options.RequestManager.ResetNumberOfRequests());

                var metric = store1.GetRequestTimeMetricForUrl(store1.Url.ForDatabase(store1.DefaultDatabase));

                UpdateMetric(metric, 150, 60);

                store1.DatabaseCommands.Get("keys/1");

                Assert.Equal(0, servers[0].Options.RequestManager.NumberOfRequests);
                Assert.Equal(1, servers[1].Options.RequestManager.NumberOfRequests);

                store1.DatabaseCommands.Get("keys/1");

                Assert.Equal(0, servers[0].Options.RequestManager.NumberOfRequests);
                Assert.Equal(2, servers[1].Options.RequestManager.NumberOfRequests);

                store1.DatabaseCommands.Put("keys/1", null, new RavenJObject(), new RavenJObject());

                Assert.Equal(1, servers[0].Options.RequestManager.NumberOfRequests);
                Assert.Equal(2, servers[1].Options.RequestManager.NumberOfRequests);

                UpdateMetric(metric, 50, 60);

                store1.DatabaseCommands.Get("keys/1");

                Assert.Equal(2, servers[0].Options.RequestManager.NumberOfRequests);
                Assert.Equal(2, servers[1].Options.RequestManager.NumberOfRequests);
            }
        }

        [Fact]
        public void RequestsWillBumpRequestTimeMetric()
        {
            using (var store1 = CreateStore(configureStore: s => s.Conventions.FailoverBehavior = FailoverBehavior.AllowReadFromSecondariesWhenRequestTimeThresholdIsSurpassed))
            {
                var metric = store1.GetRequestTimeMetricForUrl(store1.Url.ForDatabase(store1.DefaultDatabase));

                UpdateMetric(metric, 0, 60);

                var rate = metric.Rate();
                Assert.True(rate <= 0.25, "The rate is " + rate);

                for (var i = 0; i < 100; i++)
                    store1.DatabaseCommands.Get("keys/1");

                var newRate = metric.Rate();
                Assert.True(newRate > rate, string.Format("{0} > {1}", newRate, rate));
            }
        }

        [Fact]
        public async Task WillSwitchBackToPrimary()
        {
            using (var store1 = CreateStore(configureStore: s => s.Conventions.FailoverBehavior = FailoverBehavior.AllowReadFromSecondariesWhenRequestTimeThresholdIsSurpassed))
            using (var store2 = CreateStore())
            {
                SetupReplication(store1.DatabaseCommands, store2);

                var replicationInformer = store1.GetReplicationInformerForDatabase();
                replicationInformer.ClearReplicationInformationLocalCache((ServerClient)store1.DatabaseCommands);
                replicationInformer.RefreshReplicationInformation((ServerClient)store1.DatabaseCommands);

                await PauseReplicationAsync(0, store1.DefaultDatabase);
                await PauseReplicationAsync(1, store2.DefaultDatabase);

                servers.ForEach(server => server.Options.RequestManager.ResetNumberOfRequests());

                var metric = store1.GetRequestTimeMetricForUrl(store1.Url.ForDatabase(store1.DefaultDatabase));

                UpdateMetric(metric, 150, 60);

                store1.DatabaseCommands.Get("keys/1");

                Assert.Equal(0, servers[0].Options.RequestManager.NumberOfRequests);
                Assert.Equal(1, servers[1].Options.RequestManager.NumberOfRequests);

                var switched = false;
                for (var i = 0; i < 100; i++)
                {
                    store1.DatabaseCommands.Get("keys/1");

                    if (servers[0].Options.RequestManager.NumberOfRequests <= 0)
                        continue;

                    switched = true;
                    break;
                }

                Assert.True(switched, "We expected that client will switch primary.");
            }
        }

        [Fact]
        public async Task ReadFromAllServersWithAllowReadFromSecondariesWhenRequestTimeThresholdIsSurpassed()
        {
            using (var store1 = CreateStore(configureStore: s => s.Conventions.FailoverBehavior = FailoverBehavior.ReadFromAllServers | FailoverBehavior.AllowReadFromSecondariesWhenRequestTimeThresholdIsSurpassed))
            using (var store2 = CreateStore())
            using (var store3 = CreateStore())
            {
                SetupReplication(store1.DatabaseCommands, store2, store3);

                var replicationInformer = store1.GetReplicationInformerForDatabase();
                replicationInformer.ClearReplicationInformationLocalCache((ServerClient)store1.DatabaseCommands);
                replicationInformer.RefreshReplicationInformation((ServerClient)store1.DatabaseCommands);

                await PauseReplicationAsync(0, store1.DefaultDatabase);
                await PauseReplicationAsync(1, store2.DefaultDatabase);
                await PauseReplicationAsync(2, store3.DefaultDatabase);

                servers.ForEach(server => server.Options.RequestManager.ResetNumberOfRequests());

                var metric1 = store1.GetRequestTimeMetricForUrl(store1.Url.ForDatabase(store1.DefaultDatabase));
                var metric2 = store1.GetRequestTimeMetricForUrl(replicationInformer.ReplicationDestinations[0].Url);
                var metric3 = store1.GetRequestTimeMetricForUrl(replicationInformer.ReplicationDestinations[1].Url);

                UpdateMetric(metric1, 150, 60);
                UpdateMetric(metric2, 150, 60);
                UpdateMetric(metric3, 0, 60);

                Assert.True(metric1.RateSurpassed(store1.Conventions));
                Assert.True(metric2.RateSurpassed(store1.Conventions));
                Assert.False(metric3.RateSurpassed(store1.Conventions));

                store1.DatabaseCommands.Get("keys/1");
                store1.DatabaseCommands.Get("keys/1");

                Assert.Equal(0, servers[0].Options.RequestManager.NumberOfRequests);
                Assert.Equal(0, servers[1].Options.RequestManager.NumberOfRequests);
                Assert.Equal(2, servers[2].Options.RequestManager.NumberOfRequests);

                UpdateMetric(metric1, 0, 60);
                UpdateMetric(metric2, 150, 60);
                UpdateMetric(metric3, 150, 60);

                Assert.False(metric1.RateSurpassed(store1.Conventions));
                Assert.True(metric2.RateSurpassed(store1.Conventions));
                Assert.True(metric3.RateSurpassed(store1.Conventions));

                store1.DatabaseCommands.Get("keys/1");
                store1.DatabaseCommands.Get("keys/1");

                Assert.Equal(2, servers[0].Options.RequestManager.NumberOfRequests);
                Assert.Equal(0, servers[1].Options.RequestManager.NumberOfRequests);
                Assert.Equal(2, servers[2].Options.RequestManager.NumberOfRequests);

                UpdateMetric(metric1, 150, 60);
                UpdateMetric(metric2, 0, 60);
                UpdateMetric(metric3, 150, 60);

                Assert.True(metric1.RateSurpassed(store1.Conventions));
                Assert.False(metric2.RateSurpassed(store1.Conventions));
                Assert.True(metric3.RateSurpassed(store1.Conventions));

                store1.DatabaseCommands.Get("keys/1");
                store1.DatabaseCommands.Get("keys/1");

                Assert.Equal(2, servers[0].Options.RequestManager.NumberOfRequests);
                Assert.Equal(2, servers[1].Options.RequestManager.NumberOfRequests);
                Assert.Equal(2, servers[2].Options.RequestManager.NumberOfRequests);
            }
        }

        private static void UpdateMetric(IRequestTimeMetric metric, long value, int numberOfRequests)
        {
            for (var i = 0; i < numberOfRequests; i++)
            {
                metric.Update(value);
            }
        }
    }
}
