using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Policies
{
    public class TimeSeriesConfigurationTests : ReplicationTestBase
    {
        public TimeSeriesConfigurationTests(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public async Task CanConfigureTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RollupPolicies = new List<RollupPolicy>
                            {
                                new RollupPolicy(TimeSpan.FromHours(1),TimeSpan.FromHours(12)),
                                new RollupPolicy(TimeSpan.FromMinutes(1),TimeSpan.FromMinutes(180)),
                                new RollupPolicy(TimeSpan.FromSeconds(1),TimeSpan.FromSeconds(60)),
                                new RollupPolicy(TimeSpan.FromDays(1),TimeSpan.FromDays(2)),
                            },
                            RawDataRetentionTime = TimeSpan.FromHours(96)
                        },
                        
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;
                var collection = updated.Collections["Users"];

                var policies = collection.RollupPolicies;
                Assert.Equal(4, policies.Count);

                Assert.Equal(TimeSpan.FromSeconds(60), policies[0].RetentionTime);
                Assert.Equal(TimeSpan.FromSeconds(1), policies[0].AggregationTime);

                Assert.Equal(TimeSpan.FromMinutes(180), policies[1].RetentionTime);
                Assert.Equal(TimeSpan.FromMinutes(1), policies[1].AggregationTime);

                Assert.Equal(TimeSpan.FromHours(12), policies[2].RetentionTime);
                Assert.Equal(TimeSpan.FromHours(1), policies[2].AggregationTime);

                Assert.Equal(TimeSpan.FromDays(2), policies[3].RetentionTime);
                Assert.Equal(TimeSpan.FromDays(1), policies[3].AggregationTime);
            }
        }

        [Fact]
        public async Task CanExecuteRollupInTheCluster()
        {
            var cluster = await CreateRaftCluster(3);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                var p1 = new RollupPolicy(TimeSpan.FromSeconds(1));
                var p2 = new RollupPolicy(TimeSpan.FromSeconds(10));
                var p3 = new RollupPolicy(TimeSpan.FromMinutes(1));
                var p4 = new RollupPolicy(TimeSpan.FromMinutes(5));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RollupPolicies = new List<RollupPolicy>
                            {
                                p1,p2,p3,p4
                            },
                            RawDataRetentionTime = TimeSpan.FromHours(96)
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var baseline = DateTime.Today.AddDays(-1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddSeconds(0.5 * i), "watches/fitbit", new[] {29d * i});
                    }

                    session.SaveChanges();
                }

                var record = await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database));
                var primary = cluster.Nodes.Single(n => n.ServerStore.NodeTag == record.Topology.Members[0]);

                var database = await primary.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                await database.TimeSeriesPolicyRunner.RunRollUps();

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User {Name = "Karmel"}, "foo/bar");
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(100, ts.Count);

                    var ts1 = session.TimeSeriesFor("users/karmel").Get(p1.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(50, ts1.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel").Get(p2.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(5, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel").Get(p3.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(1, ts3.Count);

                    var ts4 = session.TimeSeriesFor("users/karmel").Get(p4.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(1, ts4.Count);
                }
            }
        }


        [Fact]
        public async Task CanExecuteSimpleRollup()
        {
            using (var store = GetDocumentStore())
            {
                var p1 = new RollupPolicy(TimeSpan.FromSeconds(1));
                var p2 = new RollupPolicy(TimeSpan.FromSeconds(2));
                var p3 = new RollupPolicy(TimeSpan.FromSeconds(3));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RollupPolicies = new List<RollupPolicy>
                            {
                                p1,p2,p3
                            },
                            RawDataRetentionTime = TimeSpan.FromHours(96)
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var baseline = DateTime.Today.AddDays(-1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddSeconds(0.3 * i), "watches/fitbit", new[] {29d * i});
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollUps();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    var tsSeconds = (int)(ts.Last().Timestamp - ts.First().Timestamp).TotalSeconds;

                    var ts1 = session.TimeSeriesFor("users/karmel").Get(p1.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    var ts1Seconds = (int)(ts1.Last().Timestamp - ts1.First().Timestamp).TotalSeconds;
                    Assert.Equal(ts1Seconds, tsSeconds);

                    var ts2 = session.TimeSeriesFor("users/karmel").Get(p2.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(ts1.Count / 2, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel").Get(p3.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(ts1.Count / 3, ts3.Count);
                }
            }
        }

        [Fact]
        public async Task CanExecuteRawRetention()
        {
            using (var store = GetDocumentStore())
            {
                var retention = TimeSpan.FromHours(96);
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawDataRetentionTime = retention
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var baseline = DateTime.UtcNow.Add(-retention * 2);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddHours(i), "watches/fitbit", new[] {29d * i});
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(3, ts.Count);
                }
            }
        }

        [Fact]
        public async Task CanReExecuteRollupWhenOldValuesChanged()
        {
            using (var store = GetDocumentStore())
            {
                var p1 = new RollupPolicy(TimeSpan.FromSeconds(1));
                var p2 = new RollupPolicy(TimeSpan.FromSeconds(2));
                var p3 = new RollupPolicy(TimeSpan.FromSeconds(3));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RollupPolicies = new List<RollupPolicy>
                            {
                                p1,p2,p3
                            },
                            RawDataRetentionTime = TimeSpan.FromHours(96)
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var baseline = DateTime.Today.AddDays(-1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddSeconds(0.2 * i), "watches/fitbit", new[] {29d * i});
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollUps();

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddSeconds(0.2 * i + 0.1), "watches/fitbit", new[] {29d * i});
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollUps();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(200, ts.Count);

                    var ts1 = session.TimeSeriesFor("users/karmel").Get(p1.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(20, ts1.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel").Get(p2.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(10, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel").Get(p3.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(7, ts3.Count);
                }
            }
        }

         [Fact]
        public async Task CanRemoveConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.AddDays(-1);

                var p1 = new RollupPolicy(TimeSpan.FromSeconds(1));
                var p2 = new RollupPolicy(TimeSpan.FromSeconds(2));
                var p3 = new RollupPolicy(TimeSpan.FromSeconds(3));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RollupPolicies = new List<RollupPolicy>
                            {
                                p1, p2 ,p3
                            },
                            RawDataRetentionTime = TimeSpan.FromHours(96)
                        },
                    }
                };
                
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddSeconds(0.2 * i), "watches/fitbit", new[] {29d * i});
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollUps();

                config.Collections["Users"].RollupPolicies.Remove(p3);
                config.Collections["Users"].RollupPolicies.Remove(p2);
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollUps();

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(100, ts.Count);

                    var ts1 = session.TimeSeriesFor("users/karmel").Get(p1.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(20, ts1.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel").Get(p2.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(0, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel").Get(p3.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(0, ts3.Count);
                }
            }
        }

        [Fact]
        public async Task CanAddConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.AddDays(-1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddSeconds(0.2 * i), "watches/fitbit", new[] {29d * i});
                    }
                    session.SaveChanges();
                }

                var p1 = new RollupPolicy(TimeSpan.FromSeconds(1));
                var p2 = new RollupPolicy(TimeSpan.FromSeconds(2));
                var p3 = new RollupPolicy(TimeSpan.FromSeconds(3));
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RollupPolicies = new List<RollupPolicy>
                            {
                                p1, p2 ,p3
                            },
                            RawDataRetentionTime = TimeSpan.FromHours(96)
                        },
                    }
                };
                
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                var database = await GetDocumentDatabaseInstanceFor(store);

                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollUps();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(100, ts.Count);

                    var ts1 = session.TimeSeriesFor("users/karmel").Get(p1.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(20, ts1.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel").Get(p2.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(10, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel").Get(p3.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(7, ts3.Count);
                }
            }
        }
    }
}
