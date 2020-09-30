using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Operations.TransactionsRecording;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide.Operations;
using Raven.Server.NotificationCenter.Notifications;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Client.TimeSeries.Query;
using SlowTests.Client.TimeSeries.Replication;
using Sparrow;
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
                    new TimeSeriesPolicy("ByHourFor12Hours",TimeValue.FromHours(1), TimeValue.FromHours(48)),
                    new TimeSeriesPolicy("ByMinuteFor3Hours",TimeValue.FromMinutes(1), TimeValue.FromMinutes(180)),
                    new TimeSeriesPolicy("BySecondFor1Minute",TimeValue.FromSeconds(1), TimeValue.FromSeconds(60)),
                    new TimeSeriesPolicy("ByMonthFor1Year",TimeValue.FromMonths(1), TimeValue.FromYears(1)),
                    new TimeSeriesPolicy("ByYearFor3Years",TimeValue.FromYears(1), TimeValue.FromYears(3)),
                    new TimeSeriesPolicy("ByDayFor1Month",TimeValue.FromDays(1), TimeValue.FromMonths(1)),
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                
                config.Collections["Users"].RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromHours(96));
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));


                var updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;
                var collection = updated.Collections["Users"];

                var policies = collection.Policies;
                Assert.Equal(6, policies.Count);

                Assert.Equal(TimeValue.FromSeconds(60), policies[0].RetentionTime);
                Assert.Equal(TimeValue.FromSeconds(1), policies[0].AggregationTime);

                Assert.Equal(TimeValue.FromMinutes(180), policies[1].RetentionTime);
                Assert.Equal(TimeValue.FromMinutes(1), policies[1].AggregationTime);

                Assert.Equal(TimeValue.FromHours(48), policies[2].RetentionTime);
                Assert.Equal(TimeValue.FromHours(1), policies[2].AggregationTime);

                Assert.Equal(TimeValue.FromMonths(1), policies[3].RetentionTime);
                Assert.Equal(TimeValue.FromDays(1), policies[3].AggregationTime);

                Assert.Equal(TimeValue.FromYears(1), policies[4].RetentionTime);
                Assert.Equal(TimeValue.FromMonths(1), policies[4].AggregationTime);

                Assert.Equal(TimeValue.FromYears(3), policies[5].RetentionTime);
                Assert.Equal(TimeValue.FromYears(1), policies[5].AggregationTime);
            }
        }

        [Fact]
        public async Task TimeSeriesConfigurationNotChanged()
        {
            using (var store = GetDocumentStore())
            {
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                new TimeSeriesPolicy("ByHourFor12Hours", TimeValue.FromHours(1), TimeValue.FromHours(48)),
                                new TimeSeriesPolicy("ByYearFor3Years",TimeValue.FromYears(1), TimeValue.FromYears(3)),
                                new TimeSeriesPolicy("ByDayFor1Month",TimeValue.FromDays(1), TimeValue.FromMonths(12)),
                            }
                        },
                    },
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                var db = await GetDocumentDatabaseInstanceFor(store);
                var runner = db.TimeSeriesPolicyRunner;


                config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                new TimeSeriesPolicy("ByYearFor3Years",TimeValue.FromYears(1), TimeValue.FromYears(3)),
                                new TimeSeriesPolicy("ByHourFor12Hours", TimeValue.FromHours(1), TimeValue.FromHours(48)),
                                new TimeSeriesPolicy("ByDayFor1Month",TimeValue.FromDays(1), TimeValue.FromMonths(12)),
                            },
                            RawPolicy = RawTimeSeriesPolicy.Default
                        }
                    },
                    PolicyCheckFrequency = TimeSpan.FromMinutes(10),
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                var runner2 = db.TimeSeriesPolicyRunner;

                Assert.Equal(runner, runner2);

            }
        }

        [Fact]
        public async Task CanConfigureTimeSeries2()
        {
            using (var store = GetDocumentStore())
            {
                var collectionName = "Users";
                var policies = new List<TimeSeriesPolicy>
                {
                    new TimeSeriesPolicy("BySecondFor1Minute",TimeValue.FromSeconds(1), TimeValue.FromSeconds(60)),
                    new TimeSeriesPolicy("ByMinuteFor3Hours",TimeValue.FromMinutes(1), TimeValue.FromMinutes(180)),
                    new TimeSeriesPolicy("ByHourFor12Hours",TimeValue.FromHours(1), TimeValue.FromHours(48)),
                    new TimeSeriesPolicy("ByDayFor1Month",TimeValue.FromDays(1), TimeValue.FromMonths(1)),
                    new TimeSeriesPolicy("ByMonthFor1Year",TimeValue.FromMonths(1), TimeValue.FromYears(1)),
                    new TimeSeriesPolicy("ByYearFor3Years",TimeValue.FromYears(1), TimeValue.FromYears(3)),
                };

                foreach (var policy in policies)
                {
                    await store.Maintenance.SendAsync(new ConfigureTimeSeriesPolicyOperation(collectionName, policy));
                }
                
                await store.Maintenance.SendAsync(new ConfigureRawTimeSeriesPolicyOperation(collectionName, new RawTimeSeriesPolicy(TimeValue.FromHours(96))));

                var parameters = new ConfigureTimeSeriesValueNamesOperation.Parameters
                {
                    Collection = collectionName,
                    TimeSeries = "HeartRate",
                    ValueNames = new[] {"HeartRate"},
                    Update = true
                };
                var nameConfig = new ConfigureTimeSeriesValueNamesOperation(parameters);
                await store.Maintenance.SendAsync(nameConfig);

                var updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;
                var collection = updated.Collections[collectionName];
                policies = collection.Policies;

                Assert.Equal(6, policies.Count);

                Assert.Equal(TimeValue.FromSeconds(60), policies[0].RetentionTime);
                Assert.Equal(TimeValue.FromSeconds(1), policies[0].AggregationTime);

                Assert.Equal(TimeValue.FromMinutes(180), policies[1].RetentionTime);
                Assert.Equal(TimeValue.FromMinutes(1), policies[1].AggregationTime);

                Assert.Equal(TimeValue.FromHours(48), policies[2].RetentionTime);
                Assert.Equal(TimeValue.FromHours(1), policies[2].AggregationTime);

                Assert.Equal(TimeValue.FromMonths(1), policies[3].RetentionTime);
                Assert.Equal(TimeValue.FromDays(1), policies[3].AggregationTime);

                Assert.Equal(TimeValue.FromYears(1), policies[4].RetentionTime);
                Assert.Equal(TimeValue.FromMonths(1), policies[4].AggregationTime);

                Assert.Equal(TimeValue.FromYears(3), policies[5].RetentionTime);
                Assert.Equal(TimeValue.FromYears(1), policies[5].AggregationTime);

                Assert.NotNull(updated.NamedValues);
                Assert.Equal(1, updated.NamedValues.Count);
                var mapper = updated.GetNames(collectionName, "heartrate");

                Assert.NotNull(mapper);
                Assert.Equal(1, mapper.Length);
                Assert.Equal("HeartRate", mapper[0]);
            }
        }

        [Fact]
        public async Task CanConfigureTimeSeries3()
        {
            using (var store = GetDocumentStore())
            {
                await store.TimeSeries.SetPolicyAsync<User>("By15SecondsFor1Minute", TimeValue.FromSeconds(15), TimeValue.FromSeconds(60));
                await store.TimeSeries.SetPolicyAsync<User>("ByMinuteFor3Hours",TimeValue.FromMinutes(1), TimeValue.FromMinutes(180));
                await store.TimeSeries.SetPolicyAsync<User>("ByHourFor12Hours",TimeValue.FromHours(1), TimeValue.FromHours(48));
                await store.TimeSeries.SetPolicyAsync<User>("ByDayFor1Month",TimeValue.FromDays(1), TimeValue.FromMonths(1));
                await store.TimeSeries.SetPolicyAsync<User>("ByMonthFor1Year",TimeValue.FromMonths(1), TimeValue.FromYears(1));
                await store.TimeSeries.SetPolicyAsync<User>("ByYearFor3Years",TimeValue.FromYears(1), TimeValue.FromYears(3));
                
                var updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;
                var collection = updated.Collections["Users"];
                var policies = collection.Policies;

                Assert.Equal(6, policies.Count);

                Assert.Equal(TimeValue.FromSeconds(60), policies[0].RetentionTime);
                Assert.Equal(TimeValue.FromSeconds(15), policies[0].AggregationTime);

                Assert.Equal(TimeValue.FromMinutes(180), policies[1].RetentionTime);
                Assert.Equal(TimeValue.FromMinutes(1), policies[1].AggregationTime);

                Assert.Equal(TimeValue.FromHours(48), policies[2].RetentionTime);
                Assert.Equal(TimeValue.FromHours(1), policies[2].AggregationTime);

                Assert.Equal(TimeValue.FromMonths(1), policies[3].RetentionTime);
                Assert.Equal(TimeValue.FromDays(1), policies[3].AggregationTime);

                Assert.Equal(TimeValue.FromYears(1), policies[4].RetentionTime);
                Assert.Equal(TimeValue.FromMonths(1), policies[4].AggregationTime);

                Assert.Equal(TimeValue.FromYears(3), policies[5].RetentionTime);
                Assert.Equal(TimeValue.FromYears(1), policies[5].AggregationTime);

                var ex = await Assert.ThrowsAsync<RavenException>(async () => await store.TimeSeries.RemovePolicyAsync<User>("ByMinuteFor3Hours"));
                Assert.Contains(
                    "System.InvalidOperationException: The policy 'By15SecondsFor1Minute' has a retention time of '60 seconds' but should be aggregated by policy 'ByHourFor12Hours' with the aggregation time frame of 60 minutes",
                    ex.Message);

                ex = await Assert.ThrowsAsync<RavenException>(async () => await store.TimeSeries.SetRawPolicyAsync<User>(TimeValue.FromSeconds(10)));
                Assert.Contains(
                    "System.InvalidOperationException: The policy 'rawpolicy' has a retention time of '10 seconds' but should be aggregated by policy 'By15SecondsFor1Minute' with the aggregation time frame of 15 seconds",
                    ex.Message);

                await store.TimeSeries.SetRawPolicyAsync<User>(TimeValue.FromMinutes(120));
                await store.TimeSeries.SetPolicyAsync<User>("By15SecondsFor1Minute", TimeValue.FromSeconds(30), TimeValue.FromSeconds(120));

                updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;
                collection = updated.Collections["Users"];
                policies = collection.Policies;

                Assert.Equal(6, policies.Count);
                Assert.Equal(TimeValue.FromSeconds(120), policies[0].RetentionTime);
                Assert.Equal(TimeValue.FromSeconds(30), policies[0].AggregationTime);

                await store.TimeSeries.RemovePolicyAsync<User>("By15SecondsFor1Minute");

                updated = (await store.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(store.Database))).TimeSeries;
                collection = updated.Collections["Users"];
                policies = collection.Policies;

                Assert.Equal(5, policies.Count);

                await store.TimeSeries.RemovePolicyAsync<User>(RawTimeSeriesPolicy.PolicyString);

            }
        }

        private class MyRawPolicy : RawTimeSeriesPolicy
        {
            public MyRawPolicy(TimeValue retention)
            {
                RetentionTime = retention;
            }
        }

        [Fact]
        public async Task NotValidConfigureShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new MyRawPolicy(TimeValue.FromMinutes(0));
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                        }
                    }
                };
                var ex = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config)));
                Assert.Contains("Retention time of the RawPolicy must be greater than zero", ex.Message);

                config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromMonths(1)),
                            Policies = new List<TimeSeriesPolicy>
                            {
                                new TimeSeriesPolicy("By30DaysFor5Years", TimeValue.FromDays(30), TimeValue.FromYears(5)),
                            }
                        }
                    }
                };

                ex = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config)));
                Assert.Contains("month might have different number of days", ex.Message);


                config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromMonths(12)),
                            Policies = new List<TimeSeriesPolicy>
                            {
                                new TimeSeriesPolicy("By365DaysFor5Years", TimeValue.FromSeconds(365 * 24 * 3600), TimeValue.FromYears(5)),
                            }
                        }
                    }
                };

                ex = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config)));
                Assert.Contains("month might have different number of days", ex.Message);


                config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromMonths(1)),
                            Policies = new List<TimeSeriesPolicy>
                            {
                                new TimeSeriesPolicy("By27DaysFor1Year", TimeValue.FromDays(27), TimeValue.FromYears(1)),
                                new TimeSeriesPolicy("By364DaysFor5Years", TimeValue.FromDays(364), TimeValue.FromYears(5)),
                            }
                        }
                    }
                };

                ex = await Assert.ThrowsAsync<RavenException>(() => store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config)));
                Assert.Contains("The aggregation time of the policy 'By364DaysFor5Years' (364 days) must be divided by the aggregation time of 'By27DaysFor1Year' (27 days) without a remainder", ex.Message);
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
                var p1 = new TimeSeriesPolicy("BySecond",TimeValue.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By10Seconds",TimeValue.FromSeconds(10));
                var p3 = new TimeSeriesPolicy("ByMinute",TimeValue.FromMinutes(1));
                var p4 = new TimeSeriesPolicy("By5Minutes",TimeValue.FromMinutes(5));

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
                            RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromHours(96))
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var baseline = DateTime.Today.AddDays(-1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddSeconds(0.5 * i), new[] {29d * i}, "watches/fitbit");
                    }

                    session.SaveChanges();
                }

                await Task.Delay(config.PolicyCheckFrequency.Value * 3);

                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    session.Store(new User {Name = "Karmel"}, "foo/bar");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(100, ts.Count);

                    var ts1 = session.TimeSeriesFor("users/karmel", p1.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(50, ts1.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel", p2.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(5, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel", p3.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(1, ts3.Count);

                    var ts4 = session.TimeSeriesFor("users/karmel", p4.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(1, ts4.Count);
                }
            }
        }

        public class BigMeasure
        {
            [TimeSeriesValue(0)] public double Measure1 { get; set; }
            [TimeSeriesValue(1)] public double Measure2 { get; set; }
            [TimeSeriesValue(2)] public double Measure3 { get; set; }
            [TimeSeriesValue(3)] public double Measure4 { get; set; }
            [TimeSeriesValue(4)] public double Measure5 { get; set; }
            [TimeSeriesValue(5)] public double Measure6 { get; set; }
        }

        public class SmallMeasure
        {
            [TimeSeriesValue(0)] public double Measure1 { get; set; }
        }

        [Fact]
        public async Task RollupWithMoreThan5ValuesShouldRaiseAlert()
        {
            using (var store = GetDocumentStore())
            {
                var p1 = new TimeSeriesPolicy("ByMinute",TimeValue.FromMinutes(1));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1
                            }
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var baseline = DateTime.Today.AddDays(-1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/KARMEL");
                    var big = session.TimeSeriesFor<BigMeasure>("users/karmel");
                    var small = session.TimeSeriesFor<SmallMeasure>("users/karmel");
                    for (int i = 0; i < 100; i++)
                    {
                            big.Append(baseline.AddSeconds(3 * i), new BigMeasure
                            {
                                Measure1 = i,
                                Measure2 = i,
                                Measure3 = i,
                                Measure4 = i,
                                Measure5 = i,
                                Measure6 = i,
                            }, "watches/fitbit");
                            small.Append(baseline.AddSeconds(3 * i) , new SmallMeasure
                            {
                                Measure1 = i
                            },"watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor<SmallMeasure>("users/karmel").Get().ToList();
                    var minutes = (int)(ts.Last().Timestamp - ts.First().Timestamp).TotalMinutes;

                    var ts1 = session.TimeSeriesRollupFor<SmallMeasure>("users/karmel", p1.Name).Get().ToList();
                    var ts1Minutes = (int)(ts1.Last().Timestamp - ts1.First().Timestamp).TotalMinutes;
                    Assert.Equal(ts1Minutes, minutes);
                }

                var key = AlertRaised.GetKey(AlertType.RollupExceedNumberOfValues, $"Users/BigMeasures");
                var alert = database.NotificationCenter.GetStoredMessage(key);
                Assert.Equal("Rollup 'ByMinute' for time-series 'BigMeasures' in document 'users/KARMEL' failed.", alert);
            }
        }

        [Fact]
        public async Task RollupWithMoreThan5ValuesShouldHalt()
        {
            using (var store = GetDocumentStore())
            {
                var p1 = new TimeSeriesPolicy("ByMinute",TimeValue.FromMinutes(1));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1
                            }
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var baseline = DateTime.Today.AddDays(-1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");
                    var big = session.TimeSeriesFor<BigMeasure>("users/karmel");
                    var small = session.TimeSeriesFor<SmallMeasure>("users/karmel");
                    for (int i = 0; i < 100; i++)
                    {
                            big.Append(baseline.AddSeconds(3 * i), new BigMeasure
                            {
                                Measure1 = i,
                                Measure2 = i,
                                Measure3 = i,
                                Measure4 = i,
                                Measure5 = i,
                                Measure6 = i,
                            }, "watches/fitbit");
                            small.Append(baseline.AddSeconds(3 * i) , new SmallMeasure
                            {
                                Measure1 = i
                            },"watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await WaitForPolicyRunner(database);

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor<SmallMeasure>("users/karmel").Get().ToList();
                    var minutes = (int)(ts.Last().Timestamp - ts.First().Timestamp).TotalMinutes;

                    var ts1 = session.TimeSeriesRollupFor<SmallMeasure>("users/karmel", p1.Name).Get().ToList();
                    var ts1Minutes = (int)(ts1.Last().Timestamp - ts1.First().Timestamp).TotalMinutes;
                    Assert.Equal(ts1Minutes, minutes);

                    Assert.Null(session.TimeSeriesRollupFor<BigMeasure>("users/karmel", p1.Name).Get()?.ToList());
                }

                var key = AlertRaised.GetKey(AlertType.RollupExceedNumberOfValues, $"Users/BigMeasures");
                var alert = database.NotificationCenter.GetStoredMessage(key);
                Assert.NotNull(alert);

                using (var session = store.OpenSession())
                {
                    var big = session.TimeSeriesFor("users/karmel", "BigMeasures");
                    for (int i = 100; i < 200; i++)
                    {
                        big.Append(baseline.AddHours(12).AddSeconds(3 * i), i , "watches/fitbit");
                    }
                    session.SaveChanges();
                    await database.TimeSeriesPolicyRunner.RunRollups();
                    Assert.Null(session.TimeSeriesRollupFor<BigMeasure>("users/karmel", p1.Name).Get()?.ToList());
                }

                // also retention for this TS will be stopped
                config.Collections["Users"].RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromHours(1));
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                
                await WaitForPolicyRunner(database);
                using (var session = store.OpenSession())
                {
                    Assert.Null(session.TimeSeriesFor<SmallMeasure>("users/karmel").Get()?.ToList());
                    Assert.NotNull(session.TimeSeriesFor<BigMeasure>("users/karmel").Get()?.ToList());
                }

                // to make it work, let remove all entries with 6 values
                using (var session = store.OpenSession())
                {
                    var big = session.TimeSeriesFor("users/karmel", "BigMeasures");
                    big.Delete(to: baseline.AddHours(12));
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();
                using (var session = store.OpenSession())
                {
                    Assert.Null(session.TimeSeriesFor<SmallMeasure>("users/karmel").Get()?.ToList());
                    Assert.Null(session.TimeSeriesFor<BigMeasure>("users/karmel").Get()?.ToList());

                    Assert.NotNull(session.TimeSeriesRollupFor<SmallMeasure>("users/karmel", p1.Name).Get().ToList());
                    Assert.NotNull(session.TimeSeriesRollupFor<BigMeasure>("users/karmel", p1.Name).Get().ToList());
                }
            }
        }

        [Fact]
        public async Task CanExecuteSimpleRollup()
        {
            using (var store = GetDocumentStore())
            {
                var p1 = new TimeSeriesPolicy("BySecond",TimeValue.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By2Seconds",TimeValue.FromSeconds(2));
                var p3 = new TimeSeriesPolicy("By4Seconds",TimeValue.FromSeconds(4));

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
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddSeconds(0.4 * i), new[] {29d * i}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    var tsSeconds = (int)(ts.Last().Timestamp - ts.First().Timestamp).TotalSeconds;

                    var ts1 = session.TimeSeriesFor("users/karmel", p1.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    var ts1Seconds = (int)(ts1.Last().Timestamp - ts1.First().Timestamp).TotalSeconds;
                    Assert.Equal(ts1Seconds, tsSeconds);

                    var ts2 = session.TimeSeriesFor("users/karmel", p2.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(ts1.Count / 2, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel", p3.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(ts1.Count / 4, ts3.Count);
                }
            }
        }

        [Fact]
        public async Task CanExecuteRawRetention()
        {
            using (var store = GetDocumentStore())
            {
                var retention = TimeValue.FromHours(96);
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromHours(96))
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
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddHours(i), 29 * i, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await WaitForPolicyRunner(database);

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(3, ts.Count);
                }
            }
        }

        [Fact]
        public async Task CanReExecuteRollupWhenOldValuesChanged()
        {
            using (var store = GetDocumentStore())
            {
                var p1 = new TimeSeriesPolicy("BySecond",TimeValue.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By2Seconds",TimeValue.FromSeconds(2));
                var p3 = new TimeSeriesPolicy("By4Seconds",TimeValue.FromSeconds(4));

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
                            RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromHours(96))
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
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddSeconds(0.2 * i), new[] {29d * i}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddSeconds(0.2 * i + 0.1), new[] {29d * i}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(200, ts.Count);

                    var ts1 = session.TimeSeriesFor("users/karmel", p1.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(20, ts1.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel", p2.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(10, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel", p3.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(5, ts3.Count);
                }
            }
        }

        [Fact]
        public async Task RemoveConfigurationWillKeepData()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.AddDays(-1);

                var p1 = new TimeSeriesPolicy("BySecond",TimeValue.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By2Seconds",TimeValue.FromSeconds(2));
                var p3 = new TimeSeriesPolicy("By4Seconds",TimeValue.FromSeconds(4));

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
                            RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromHours(96))
                        },
                    }
                };
                
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddSeconds(0.2 * i), new[] {29d * i}, "watches/fitbit");
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
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(100, ts.Count);

                    var ts1 = session.TimeSeriesFor("users/karmel", p1.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(20, ts1.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel", p2.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(10, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel", p3.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(5, ts3.Count);
                }

                using (var session = store.OpenSession())
                {
                    for (int i = 100; i < 200; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append( baseline.AddSeconds(0.2 * i), new[] {29d * i}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(200, ts.Count);

                    var ts1 = session.TimeSeriesFor("users/karmel", p1.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(40, ts1.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel", p2.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(10, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel", p3.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(5, ts3.Count);
                }
            }
        }

        [Fact]
        public async Task CanRemoveConfigurationEntirely()
        {
            using (var store = GetDocumentStore())
            {
                var baseline = DateTime.Today.AddDays(-1);

                var p1 = new TimeSeriesPolicy("BySecond",TimeValue.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By2Seconds",TimeValue.FromSeconds(2));
                var p3 = new TimeSeriesPolicy("By4Seconds",TimeValue.FromSeconds(4));

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
                            RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromHours(96))
                        },
                    }
                };
                
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i < 100; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddSeconds(0.2 * i), new[] {29d * i}, "watches/fitbit");
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
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(100, ts.Count);

                    var ts1 = session.TimeSeriesFor("users/karmel", p1.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(20, ts1.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel", p2.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(10, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel", p3.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(5, ts3.Count);
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
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddSeconds(0.2 * i), new[] {29d * i}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var p1 = new TimeSeriesPolicy("BySecond",TimeValue.FromSeconds(1));
                var p2 = new TimeSeriesPolicy("By2Seconds",TimeValue.FromSeconds(2));
                var p3 = new TimeSeriesPolicy("By4Seconds",TimeValue.FromSeconds(4));

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
                            RawPolicy = new RawTimeSeriesPolicy(TimeValue.FromHours(96))
                        },
                    }
                };
                
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                var database = await GetDocumentDatabaseInstanceFor(store);

                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get().ToList();
                    Assert.Equal(100, ts.Count);

                    var ts1 = session.TimeSeriesFor("users/karmel", p1.GetTimeSeriesName("Heartrate")).Get().ToList();
                    Assert.Equal(20, ts1.Count);

                    var ts2 = session.TimeSeriesFor("users/karmel", p2.GetTimeSeriesName("Heartrate")).Get().ToList();
                    Assert.Equal(10, ts2.Count);

                    var ts3 = session.TimeSeriesFor("users/karmel", p3.GetTimeSeriesName("Heartrate")).Get().ToList();
                    Assert.Equal(5, ts3.Count);
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
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), new[] {29d * i, 30 * i}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var raw = new RawTimeSeriesPolicy(TimeValue.FromMinutes(30));
                var p = new TimeSeriesPolicy("By10Minutes",TimeValue.FromMinutes(10), TimeValue.FromHours(1));

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
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate")?
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Where(entry => entry.IsRollup == false)
                        .ToList();
                    Assert.NotNull(ts);
                    Assert.Equal(30, ts.Count);
                    var ts2 = session.TimeSeriesFor("users/karmel", p.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(((TimeSpan)p.RetentionTime).TotalMinutes / ((TimeSpan)p.AggregationTime).TotalMinutes, ts2.Count);
                }
            }
        }

        [Fact]
        public async Task CanRetainAndRollup2()
        {
            using (var store = GetDocumentStore())
            {
                var now = DateTime.UtcNow;
                var minutes = 1440;
                var baseline = now.AddMinutes(-minutes);

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");
                    for (int i = 0; i <= minutes; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), new[] {29d * i, 30 * i}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var raw = new RawTimeSeriesPolicy(TimeValue.FromMinutes(30));
                var p = new TimeSeriesPolicy("By10Minutes",TimeValue.FromMinutes(10), TimeValue.FromHours(3));
                var p2 = new TimeSeriesPolicy("ByHour",TimeValue.FromHours(1), TimeValue.FromHours(12));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p, p2
                            }
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                
                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();
                WaitForUserToContinueTheTest(store);
                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate")?
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Where(entry => entry.IsRollup == false)
                        .ToList();
                    Assert.NotNull(ts);
                    Assert.Equal(30, ts.Count);
                    var ts1 = session.TimeSeriesFor("users/karmel", p.GetTimeSeriesName("Heartrate")).Get().ToList();
                    var ts2 = session.TimeSeriesFor("users/karmel", p2.GetTimeSeriesName("Heartrate")).Get().ToList();
                    Assert.Equal(((TimeSpan)p.RetentionTime).TotalMinutes / ((TimeSpan)p.AggregationTime).TotalMinutes, ts1.Count);
                    Assert.Equal(((TimeSpan)p2.RetentionTime).TotalMinutes / ((TimeSpan)p2.AggregationTime).TotalMinutes, ts2.Count);
                }
            }
        }

        [Fact]
        public async Task CanRetainAndRollupForMonths()
        {
            using (var store = GetDocumentStore())
            {
                var now = DateTime.UtcNow;
                var baseline = now.AddMonths(-48);

                var totalDays = 365 * 4 + 1; // true for this century

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");
                    for (int i = 0; i <= 24 * totalDays; i+=3) // appr. 10,000 items
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddHours(i), new[] {29d * i, 30 * i}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                var raw = new RawTimeSeriesPolicy(TimeValue.FromDays(120));
                var p = new TimeSeriesPolicy("ByQuarterFor3Years",TimeValue.FromMonths(3), TimeValue.FromYears(3));

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
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate")?
                        .Get(DateTime.MinValue, DateTime.MaxValue)
                        .Where(entry => entry.IsRollup == false)
                        .ToList();

                    Assert.NotNull(ts);
                    Assert.Equal(960, ts.Count);
                    var ts2 = session.TimeSeriesFor("users/karmel", p.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(12, ts2.Count);
                }
            }
        }

        [Fact]
        public async Task CanRecordAndReplay()
        {
            var recordFilePath = NewDataPath();

            var raw = new RawTimeSeriesPolicy(TimeValue.FromMinutes(30));
            var p = new TimeSeriesPolicy("By10Minutes",TimeValue.FromMinutes(10), TimeValue.FromHours(1));
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
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), new[] {29d * i}, "watches/fitbit");
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
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    count1 = ts.Count;

                    ts = session.TimeSeriesFor("users/karmel", p.GetTimeSeriesName("Heartrate")).Get( DateTime.MinValue, DateTime.MaxValue).ToList();
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
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(count1, ts.Count);
                    ts = session.TimeSeriesFor("users/karmel", p.GetTimeSeriesName("Heartrate")).Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(count2, ts.Count);

                }
            }
        }

        [Fact]
        public async Task FullRetentionAndRollup()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeValue.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours",TimeValue.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day",TimeValue.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes",TimeValue.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour",TimeValue.FromMinutes(60), raw.RetentionTime * 3);

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
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var now = DateTime.UtcNow;
                var baseline = now.AddDays(-12);
                var total = ((TimeSpan)TimeValue.FromDays(12)).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");
                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), new[] {29d * i, i}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }
                
                WaitForUserToContinueTheTest(store);

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await QueryFromMultipleTimeSeries.VerifyFullPolicyExecution(store, config.Collections["Users"]);
            }
        }

        [Fact]
        public async Task RapidRetentionAndRollupInACluster()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                var raw = new RawTimeSeriesPolicy(TimeValue.FromSeconds(15));

                var p1 = new TimeSeriesPolicy("By1",TimeValue.FromSeconds(1), raw.RetentionTime * 2);
                var p2 = new TimeSeriesPolicy("By2",TimeValue.FromSeconds(2), raw.RetentionTime * 3);
                var p3 = new TimeSeriesPolicy("By4",TimeValue.FromSeconds(4), raw.RetentionTime * 4);
                var p4 = new TimeSeriesPolicy("By8",TimeValue.FromSeconds(8), raw.RetentionTime * 5);

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
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };

                var now = DateTime.UtcNow;
                var baseline = now.AddSeconds(-15 * 3);
                var total = ((TimeSpan)TimeValue.FromSeconds(15 * 3)).TotalMilliseconds;

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMilliseconds(i), new[] {29d * i, i}, "watches/fitbit");
                    }
                    session.SaveChanges();

                    session.Store(new User {Name = "Karmel"}, "marker");
                    session.SaveChanges();

                    await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "marker", null, TimeSpan.FromSeconds(15));
                }

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                await Task.Delay((TimeSpan)(p4.RetentionTime + TimeValue.FromSeconds(10)));
                // nothing should be left

                foreach (var node in cluster.Nodes)
                {
                    using (var nodeStore = GetDocumentStore(new Options
                    {
                        Server = node,
                        CreateDatabase =  false,
                        DeleteDatabaseOnDispose = false,
                        ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                        {
                            DisableTopologyUpdates = true
                        },
                        ModifyDatabaseName = _ => store.Database
                    }))
                    {
                        using (var session = nodeStore.OpenSession())
                        {
                            var user = session.Load<User>("users/karmel");
                            Assert.Equal(0,session.Advanced.GetTimeSeriesFor(user)?.Count ?? 0);
                            var db = await node.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                            await TimeSeriesReplicationTests.AssertNoLeftOvers(db);
                        }
                    }
                }
            }
        }

        [Fact]
        public async Task RapidRetentionAndRollup()
        {
            using (var store = GetDocumentStore())
            {
                var raw = new RawTimeSeriesPolicy(TimeValue.FromSeconds(15));

                var p1 = new TimeSeriesPolicy("By1",TimeValue.FromSeconds(1), raw.RetentionTime * 2);
                var p2 = new TimeSeriesPolicy("By2",TimeValue.FromSeconds(2), raw.RetentionTime * 3);
                var p3 = new TimeSeriesPolicy("By4",TimeValue.FromSeconds(4), raw.RetentionTime * 4);
                var p4 = new TimeSeriesPolicy("By8",TimeValue.FromSeconds(8), raw.RetentionTime * 5);

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
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(1)
                };

                var now = DateTime.UtcNow;
                var baseline = now.AddSeconds(-15 * 3);
                var total = ((TimeSpan)TimeValue.FromSeconds(15 * 3)).TotalMilliseconds;

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMilliseconds(i), new[] {29d * i, i}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                WaitForUserToContinueTheTest(store);

                await Task.Delay((TimeSpan)(p4.RetentionTime + TimeValue.FromSeconds(10)));
                // nothing should be left

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>("users/karmel");
                    Assert.Equal(0,session.Advanced.GetTimeSeriesFor(user)?.Count ?? 0);
                }
            }
        }

        [Fact]
        public async Task SkipRollupDeadSegmentAfterCleanup()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            {
                var baseline = DateTime.Today;

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                new TimeSeriesPolicy("By1",TimeValue.FromSeconds(1))
                            }
                        },
                    },
                };
                await storeB.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                using (var session = storeA.OpenSession())
                {
                    session.Store(new User { Name = "Oren" }, "users/ayende");
                    session.TimeSeriesFor("users/ayende", "Heartrate")
                        .Append(baseline.AddMinutes(10), new double[] { 1 }, "watches/fitbit");
                    session.SaveChanges();
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                using (var session = storeA.OpenSession())
                {
                    session.TimeSeriesFor("users/ayende", "Heartrate").Delete();
                    session.TimeSeriesFor("users/ayende", "Heartrate2")
                        .Append(baseline.AddMinutes(10), new double[] { 1 }, "watches/fitbit");
                    session.SaveChanges();
                }

                EnsureReplicating(storeA, storeB);
                var b = await GetDocumentDatabaseInstanceFor(storeB);
                await b.TombstoneCleaner.ExecuteCleanup();
                await b.TimeSeriesPolicyRunner.RunRollups();
            }
        }


        [Fact]
        public async Task FullRetentionAndRollupInACluster()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                var raw = new RawTimeSeriesPolicy(TimeValue.FromHours(24));

                var p1 = new TimeSeriesPolicy("By6Hours",TimeValue.FromHours(6), raw.RetentionTime * 4);
                var p2 = new TimeSeriesPolicy("By1Day",TimeValue.FromDays(1), raw.RetentionTime * 5);
                var p3 = new TimeSeriesPolicy("By30Minutes",TimeValue.FromMinutes(30), raw.RetentionTime * 2);
                var p4 = new TimeSeriesPolicy("By1Hour",TimeValue.FromMinutes(60), raw.RetentionTime * 3);

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1
                                ,p2,p3,p4
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(5)
                };

                var now = DateTime.UtcNow;
                var baseline = now.AddDays(-12);
                var total = ((TimeSpan)TimeValue.FromDays(12)).TotalMinutes;

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddMinutes(i), new[] {29d * i, i}, "watches/fitbit");
                    }
                    session.SaveChanges();

                    session.Store(new User {Name = "Karmel"}, "marker");
                    session.SaveChanges();

                    await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "marker", null, TimeSpan.FromSeconds(15));
                }

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                await Task.Delay(config.PolicyCheckFrequency.Value * 3);
                WaitForUserToContinueTheTest(store);

                foreach (var node in cluster.Nodes)
                {
                    using (var nodeStore = GetDocumentStore(new Options
                    {
                        Server = node,
                        CreateDatabase =  false,
                        DeleteDatabaseOnDispose = false,
                        ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                        {
                            DisableTopologyUpdates = true
                        },
                        ModifyDatabaseName = _ => store.Database
                    }))
                    {
                       await QueryFromMultipleTimeSeries.VerifyFullPolicyExecution(nodeStore, config.Collections["Users"]); 
                    }
                }
            }
        }

        [Fact]
        public async Task FullRetentionAndRollupInAClusterLargeTimeSpan()
        {
            var cluster = await CreateRaftCluster(3, watcherCluster: true);
            using (var store = GetDocumentStore(new Options
            {
                Server = cluster.Leader,
                ReplicationFactor = 3
            }))
            {
                var raw = new RawTimeSeriesPolicy(TimeValue.FromHours(24));

                var p1 = new TimeSeriesPolicy("Daily",TimeValue.FromHours(24), TimeValue.FromMonths(6));
                var p2 = new TimeSeriesPolicy("Monthly",TimeValue.FromMonths(1));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            RawPolicy = raw,
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1 ,p2
                            }
                        },
                    },
                    PolicyCheckFrequency = TimeSpan.FromSeconds(5)
                };

                var now = DateTime.UtcNow;
                var baseline = now.AddYears(-3);
                var total = (now - baseline).TotalHours;

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    for (int i = 0; i <= total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddHours(i), new[] {29d * i, i}, "watches/fitbit");
                    }
                    session.SaveChanges();

                    session.Store(new User {Name = "Karmel"}, "marker");
                    session.SaveChanges();

                    await WaitForDocumentInClusterAsync<User>((DocumentSession)session, "marker", null, TimeSpan.FromSeconds(15));
                }

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                await Task.Delay(config.PolicyCheckFrequency.Value * 3);
                WaitForUserToContinueTheTest(store);

                foreach (var node in cluster.Nodes)
                {
                    using (var nodeStore = GetDocumentStore(new Options
                    {
                        Server = node,
                        CreateDatabase =  false,
                        DeleteDatabaseOnDispose = false,
                        ModifyDocumentStore = s => s.Conventions = new DocumentConventions
                        {
                            DisableTopologyUpdates = true
                        },
                        ModifyDatabaseName = _ => store.Database
                    }))
                    {
                        await WaitForValueAsync(() =>
                        {
                            using (var session = store.OpenSession())
                            {
                                var ts = session.TimeSeriesFor("users/karmel", "Heartrate")
                                    .Get()?
                                    .Where(entry => entry.IsRollup == false)
                                    .ToList();

                                Assert.NotNull(ts);
                                Assert.Equal(((TimeSpan)raw.RetentionTime).TotalHours, ts.Count);

                                var daily = session.TimeSeriesFor("users/karmel", p1.GetTimeSeriesName("Heartrate"))
                                    .Get()?
                                    .ToList();

                                Assert.NotNull(daily);
                                Assert.True(daily.Count > 168);
                                Assert.True(daily.Count < 186);

                                var monthly  = session.TimeSeriesFor("users/karmel", p2.GetTimeSeriesName("Heartrate"))
                                    .Get()?
                                    .ToList();

                                Assert.NotNull(monthly);
                                Assert.Equal(12 * 3, monthly.Count);
                            }
                            return true;
                        }, true);
                    }
                }
            }
        }

        [Fact]
        public async Task RollupLargeTime()
        {
            using (var store = GetDocumentStore())
            {

                var p = new TimeSeriesPolicy("ByDay", TimeValue.FromDays(1));

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
                var total = ((TimeSpan)TimeValue.FromDays(12)).TotalHours;

                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");
                    for (int i = 0; i < total; i++)
                    {
                        session.TimeSeriesFor("users/karmel", "Heartrate")
                            .Append(baseline.AddHours(i), new[] {29d * i}, "watches/fitbit");
                    }
                    session.SaveChanges();
                }
                
                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();
                
                using (var session = store.OpenSession())
                {
                    var ts = session.TimeSeriesFor("users/karmel", "Heartrate").Get(DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(288, ts.Count);

                    ts = session.TimeSeriesFor("users/karmel", p.GetTimeSeriesName("Heartrate")).Get( DateTime.MinValue, DateTime.MaxValue).ToList();
                    Assert.Equal(12, ts.Count);
                }
            }
        }


        [Fact]
        public async Task CanAddNewPolicyForExistingTimeSeries()
        {
            using (var store = GetDocumentStore())
            {
                
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Karmel"}, "users/karmel");

                    session.Store(new Company(), "companies/1");
                    session.TimeSeriesFor("companies/1", "Heartrate")
                        .Append(DateTime.UtcNow.AddYears(-1), new[] {29d}, "watches/fitbit");
                    session.SaveChanges();
                }

                var p = new TimeSeriesPolicy("ByYear", TimeValue.FromYears(1), TimeValue.MaxValue);
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
                        ["Companies"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p
                            }
                        },
                    },
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                using (var session = store.OpenSession())
                {
                    session.TimeSeriesFor("users/karmel", "Heartrate")
                        .Append(DateTime.UtcNow, new[] {29d}, "watches/fitbit");
                    session.SaveChanges();
                }
                
                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    var res = session.TimeSeriesFor("companies/1", "Heartrate@byyear").Get().Single();
                    Assert.Equal(6, res.Values.Length);
                }
            }
        }

        [Fact]
        public async Task CanReRollAfterRemoval()
        {
            using (var store = GetDocumentStore())
            {
                var t = DateTime.UtcNow.AddYears(-1);
                using (var session = store.OpenSession())
                {
                    session.Store(new Company(), "companies/1");
                    session.TimeSeriesFor("companies/1", "Heartrate")
                        .Append(t, new[] {29d}, "watches/fitbit");
                    session.TimeSeriesFor("companies/1", "Heartrate")
                        .Append(t.AddMinutes(1), new[] {31d}, "watches/fitbit");
                    session.SaveChanges();
                }

                var p = new TimeSeriesPolicy("ByYear", TimeValue.FromYears(1));
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Companies"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p
                            }
                        },
                    },
                };

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                
                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    var raw = session.TimeSeriesFor("companies/1", "Heartrate").Get().ToList();
                    Assert.Equal(2, raw.Count);

                    var res = session.TimeSeriesFor("companies/1", "Heartrate@byyear").Get().Single();
                    Assert.Equal(30, res.Values[4] / 2);
                    Assert.Equal(2, res.Values[5]);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Company(), "companies/1");
                    var ts = session.TimeSeriesFor("companies/1", "Heartrate");
                    ts.Delete(t);
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    var raw = session.TimeSeriesFor("companies/1", "Heartrate").Get().ToList();
                    Assert.Equal(1, raw.Count);

                    var res = session.TimeSeriesFor("companies/1", "Heartrate@byyear").Get().Single();
                    Assert.Equal(31, res.Values[4]);
                    Assert.Equal(1, res.Values[5]);
                }
            }
        }

        [Fact]
        public async Task CanReRollAfterUpdate()
        {
            using (var store = GetDocumentStore())
            {
                var t = DateTime.UtcNow.AddYears(-1);
                using (var session = store.OpenSession())
                {
                    session.Store(new Company(), "companies/1");
                    session.TimeSeriesFor("companies/1", "Heartrate")
                        .Append(t, new[] {29d}, "watches/fitbit");
                    session.TimeSeriesFor("companies/1", "Heartrate")
                        .Append(t.AddMinutes(1), new[] {31d}, "watches/fitbit");
                    session.SaveChanges();
                }

                var p = new TimeSeriesPolicy("ByYear", TimeValue.FromYears(1));
                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Companies"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p
                            }
                        },
                    },
                };

                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));
                
                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.HandleChanges();
                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    var raw = session.TimeSeriesFor("companies/1", "Heartrate").Get().ToList();
                    Assert.Equal(2, raw.Count);

                    var res = session.TimeSeriesFor("companies/1", "Heartrate@byyear").Get().Single();
                    Assert.Equal(30, res.Values[4] / 2);
                    Assert.Equal(2, res.Values[5]);
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Company(), "companies/1");
                    var ts = session.TimeSeriesFor("companies/1", "Heartrate");
                    ts.Append(t, 27d);
                    session.SaveChanges();
                }

                await database.TimeSeriesPolicyRunner.RunRollups();
                await database.TimeSeriesPolicyRunner.DoRetention();

                using (var session = store.OpenSession())
                {
                    var raw = session.TimeSeriesFor("companies/1", "Heartrate").Get().ToList();
                    Assert.Equal(2, raw.Count);

                    var res = session.TimeSeriesFor("companies/1", "Heartrate@byyear").Get().Single();
                    Assert.Equal(29, res.Values[4] / 2);
                    Assert.Equal(2, res.Values[5]);
                }
            }
        }

        [Fact]
        public async Task RollupNamesShouldKeepOriginalCasing()
        {
            using (var store = GetDocumentStore())
            {
                var p1 = new TimeSeriesPolicy("By10Seconds", TimeValue.FromSeconds(10));
                var p2 = new TimeSeriesPolicy("By1Minutes", TimeValue.FromMinutes(1));
                var p3 = new TimeSeriesPolicy("By2Hours", TimeValue.FromHours(2));

                var config = new TimeSeriesConfiguration
                {
                    Collections = new Dictionary<string, TimeSeriesCollectionConfiguration>
                    {
                        ["Users"] = new TimeSeriesCollectionConfiguration
                        {
                            Policies = new List<TimeSeriesPolicy>
                            {
                                p1, p2 ,p3
                            }
                        },
                    }
                };
                await store.Maintenance.SendAsync(new ConfigureTimeSeriesOperation(config));

                var baseline = DateTime.Today.AddHours(-4);
                var rawName = "HeartRate";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), "users/karmel");

                    for (int i = 0; i <= TimeSpan.FromHours(4).TotalSeconds; i++)
                    {
                        session.TimeSeriesFor("users/karmel", rawName)
                            .Append(baseline.AddSeconds(i), i);
                    }

                    session.SaveChanges();
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                await database.TimeSeriesPolicyRunner.RunRollups();

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<User>("users/karmel");
                    var tsNames = session.Advanced.GetTimeSeriesFor(doc);
                    Assert.Equal(4, tsNames.Count);
                    
                    Assert.Equal(rawName, tsNames[0]);
                    Assert.Equal($"{rawName}@{p1.Name}", tsNames[1]);
                    Assert.Equal($"{rawName}@{p2.Name}", tsNames[2]);
                    Assert.Equal($"{rawName}@{p3.Name}", tsNames[3]);
                }
            }
        }
    }
}
