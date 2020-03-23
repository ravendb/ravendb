using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Operations.TransactionsRecording;
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
                var config = new TimeSeriesConfiguration();
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                config.Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>();
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                config.Collections["Users"] = new TimeSeriesCollectionConfiguration();
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                config.Collections["Users"].Policies = new List<TimeSeriesPolicy>
                {
                    new TimeSeriesPolicy("ByHourFor12Hours",TimeSpan.FromHours(1), TimeSpan.FromHours(12)),
                    new TimeSeriesPolicy("ByMinuteFor3Hours",TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(180)),
                    new TimeSeriesPolicy("BySecondFor1Minute",TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(60)),
                    new TimeSeriesPolicy("ByDayFor2Days",TimeSpan.FromDays(1), TimeSpan.FromDays(2)),
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                
                config.Collections["Users"].RawPolicy = new RawTimeSeriesPolicy(TimeSpan.FromHours(96));
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));


                var updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;
                var collection = updated.Collections["Users"];

                var policies = collection.Policies;
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
                var p1 = new TimeSeriesPolicy("BySecond",TimeSpan.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By10Seconds",TimeSpan.FromSeconds(10));
                var p3 = new TimeSeriesPolicy("ByMinute",TimeSpan.FromMinutes(1));
                var p4 = new TimeSeriesPolicy("By5Minutes",TimeSpan.FromMinutes(5));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            },
                            RawPolicy = new RawTimeSeriesPolicy(TimeSpan.FromHours(96))
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
                await database.TimeSeriesPolicyRunner.RunRollups();

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
                var p1 = new TimeSeriesPolicy("BySecond",TimeSpan.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By2Seconds",TimeSpan.FromSeconds(2));
                var p3 = new TimeSeriesPolicy("By3Seconds",TimeSpan.FromSeconds(3));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3
                            }
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
                await database.TimeSeriesPolicyRunner.RunRollups();

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
                            RawPolicy = new RawTimeSeriesPolicy(TimeSpan.FromHours(96))
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
                var p1 = new TimeSeriesPolicy("BySecond",TimeSpan.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By2Seconds",TimeSpan.FromSeconds(2));
                var p3 = new TimeSeriesPolicy("By3Seconds",TimeSpan.FromSeconds(3));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3
                            },
                            RawPolicy = new RawTimeSeriesPolicy(TimeSpan.FromHours(96))
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
                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddSeconds(0.2 * i + 0.1), "watches/fitbit", new[] {29d * i});
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();

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
        public async Task RemoveConfigurationWillKeepData()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.AddDays(-1);

                var p1 = new TimeSeriesPolicy("BySecond",TimeSpan.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By2Seconds",TimeSpan.FromSeconds(2));
                var p3 = new TimeSeriesPolicy("By3Seconds",TimeSpan.FromSeconds(3));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1, p2 ,p3
                            },
                            RawPolicy = new RawTimeSeriesPolicy(TimeSpan.FromHours(96))
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
                await database.TimeSeriesPolicyRunner.RunRollups();

                config.Collections["Users"].Policies.Remove(p3);
                config.Collections["Users"].Policies.Remove(p2);
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollups();

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

                using (var session = store.OpenSession())
                {
                    for (int i = 100; i < 200; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddSeconds(0.2 * i), "watches/fitbit", new[] {29d * i});
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(200, ts.Count);

                    var ts1 = session.TimeSeriesFor("users/karmel").Get(p1.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(40, ts1.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel").Get(p2.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(10, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel").Get(p3.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(7, ts3.Count);
                }

            }
        }

        [Fact]
        public async Task CanRemoveConfigurationEntirely()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.AddDays(-1);

                var p1 = new TimeSeriesPolicy("BySecond",TimeSpan.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By2Seconds",TimeSpan.FromSeconds(2));
                var p3 = new TimeSeriesPolicy("By3Seconds",TimeSpan.FromSeconds(3));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1, p2 ,p3
                            },
                            RawPolicy = new RawTimeSeriesPolicy(TimeSpan.FromHours(96))
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
                await database.TimeSeriesPolicyRunner.RunRollups();

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(null));

                Assert.True(await WaitForValueAsync(() => database.TimeSeriesPolicyRunner == null, true));


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

                var p1 = new TimeSeriesPolicy("BySecond",TimeSpan.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By2Seconds",TimeSpan.FromSeconds(2));
                var p3 = new TimeSeriesPolicy("By3Seconds",TimeSpan.FromSeconds(3));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1, p2 ,p3
                            },
                            RawPolicy = new RawTimeSeriesPolicy(TimeSpan.FromHours(96))
                        },
                    }
                };
                
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                var database = await GetDocumentDatabaseInstanceFor(store);

                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollups();

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

        [Fact]
        public async Task CanRetainAndRollup()
        {
            using (var store = GetDocumentStore())
            {
                var now = DateTime.UtcNow;
                var baseline = now.AddMinutes(-120);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");
                    for (int i = 0; i <= 120; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddMinutes(i), "watches/fitbit", new[] {29d * i, 30 * i});
                    }
                    session.SaveChanges();
                }

                var raw = new RawTimeSeriesPolicy(TimeSpan.FromMinutes(30));
                var p = new TimeSeriesPolicy("By10Minutes",TimeSpan.FromMinutes(10), TimeSpan.FromHours(1));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p
                            }
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                
                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();
                
                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(30, ts.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel").Get(p.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(p.RetentionTime.TotalMinutes / p.AggregationTime.TotalMinutes, ts2.Count);
                }
            }
        }

        [Fact]
        public async Task CanRecordAndReplay()
        {
            var recordFilePath = NewDataPath();

            var raw = new RawTimeSeriesPolicy(TimeSpan.FromMinutes(30));
            var p = new TimeSeriesPolicy("By10Minutes",TimeSpan.FromMinutes(10), TimeSpan.FromHours(1));
            var config = new TimeSeriesConfiguration
            {
                Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                {
                    ["Users"] = new TimeSeriesCollectionConfiguration {RawPolicy = raw, Policies = new List<TimeSeriesPolicy> {p}},
                }
            };

            int count1, count2;
            using (var store = GetDocumentStore())
            {
                var now = DateTime.UtcNow;
                var baseline = now.AddHours(-2);

                store.Maintenance.Send(new StartTransactionsRecordingOperation(recordFilePath));

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");
                    for (int i = 0; i < 120; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddMinutes(i), "watches/fitbit", new[] {29d * i});
                    }
                    session.SaveChanges();
                }
               
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                store.Maintenance.Send(new StopTransactionsRecordingOperation());


                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    count1 = ts.Count;

                    ts = session.TimeSeriesFor("users/karmel").Get(p.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    count2 = ts.Count;
                }
            }

            using (var store = GetDocumentStore())
            using (var replayStream = new FileStream(recordFilePath, FileMode.Open))
            {
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var command = new GetNextOperationIdCommand();
                store.Commands().Execute(command);
                store.Maintenance.Send(new ReplayTransactionsRecordingOperation(replayStream, command.Result));

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(count1, ts.Count);
                    ts = session.TimeSeriesFor("users/karmel").Get(p.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(count2, ts.Count);

                }
            }
        }

        [Fact]
        public async Task FullRetentionAndRollup()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeSpan.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours",TimeSpan.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day",TimeSpan.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes",TimeSpan.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour",TimeSpan.FromMinutes(60), raw.RetentionTime * 3);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1,p2,p3,p4
                            }
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var now = DateTime.UtcNow;
                var baseline = now.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");
                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddMinutes(i), "watches/fitbit", new[] {29d * i, i});
                    }
                    session.SaveChanges();
                }
                
                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();
                
                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(raw.RetentionTime.TotalMinutes, ts.Count);

                    ts = session.TimeSeriesFor("users/karmel").Get(p3.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(p3.RetentionTime.TotalMinutes / p3.AggregationTime.TotalMinutes, ts.Count);

                    ts = session.TimeSeriesFor("users/karmel").Get(p4.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(p4.RetentionTime.TotalMinutes / p4.AggregationTime.TotalMinutes, ts.Count);

                    ts = session.TimeSeriesFor("users/karmel").Get(p1.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(p1.RetentionTime.TotalMinutes / p1.AggregationTime.TotalMinutes, ts.Count);

                    ts = session.TimeSeriesFor("users/karmel").Get(p2.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(p2.RetentionTime.TotalMinutes / p2.AggregationTime.TotalMinutes, ts.Count);
                }
            }
        }

         [Fact]
        public async Task RollupLargeTime()
        {
            using (var store = GetDocumentStore())
            {

                var p = new TimeSeriesPolicy("ByDay", TimeSpan.FromDays(1));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p
                            }
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var baseline = DateTime.UtcNow.AddDays(-12);
                var total = TimeSpan.FromDays(12).TotalHours;

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");
                    for (int i = 0; i < total; i++)
                    {
                        session.TimeSeriesFor("users/karmel")
                            .Append("Heartrate", baseline.AddHours(i), "watches/fitbit", new[] {29d * i});
                    }
                    session.SaveChanges();
                }
                
                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();
                
                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel").Get("Heartrate", DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(288, ts.Count);

                    ts = session.TimeSeriesFor("users/karmel").Get(p.GetTimeSeriesName("Heartrate"), DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(12, ts.Count);
                }
            }
        }
    }
}
