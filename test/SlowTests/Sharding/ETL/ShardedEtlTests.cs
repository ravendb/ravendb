using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Elastic.Clients.Elasticsearch;
using Elastic.Clients.Elasticsearch.IndexManagement;
using Elastic.Transport.Products.Elasticsearch;
using FastTests;
using FastTests.Client;
using Microsoft.Data.SqlClient;
using Parquet;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Exceptions;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.ElasticSearch;
using Raven.Server.Documents.ETL.Providers.OLAP;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Raven.Server.Documents.Sharding;
using Raven.Server.ServerWide.Context;
using Raven.Server.SqlMigration;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using SlowTests.Server.Documents.ETL.Olap;
using SlowTests.Server.Documents.ETL.Raven;
using SlowTests.Server.Documents.ETL.SQL;
using SlowTests.Server.Documents.Migration;
using Tests.Infrastructure;
using Tests.Infrastructure.ConnectionString;
using Tests.Infrastructure.Entities;
using Employee = Orders.Employee;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.ETL
{
    public class ShardedEtlTests : RavenTestBase
    {
        public ShardedEtlTests(ITestOutputHelper output) : base(output)
        {
        }

        private const string DefaultFrequency = "* * * * *"; // every minute
        private const string AllFilesPattern = "*.*";

        private const string OrderIndexName = "orders";
        private const string OrderLinesIndexName = "orderlines";

        private const string DefaultScript = @"
var orderData = {
    Id: id(this),
    OrderLinesCount: this.Lines.length,
    TotalCost: 0
};

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    orderData.TotalCost += line.PricePerUnit;
    loadToOrderLines({
        OrderId: id(this),
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.PricePerUnit
    });
}

loadToOrders(orderData);
";

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void RavenEtl_Unsharded_Destination()
        {
            using (var src = Sharding.GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                Server.ServerStore.Engine.Timeout.Disable = true;

                SetupRavenEtl(src, dest, "Users", script: null);

                var etlDone = Sharding.Etl.WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                const string id = "users/1";

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Joe Doe"
                    }, id);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(id);

                    Assert.NotNull(user);
                    Assert.Equal("Joe Doe", user.Name);
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete(id);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(id);

                    Assert.Null(user);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void RavenEtl_Unsharded_Destination2()
        {
            using (var src = Sharding.GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                Server.ServerStore.Engine.Timeout.Disable = true;

                SetupRavenEtl(src, dest, "Users", script: null);

                var etlsDone = Sharding.Etl.WaitForEtlOnAllShards(src, (n, s) => s.LoadSuccesses > 0);
                var dbRecord = src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(src.Database)).Result;
                var shardedCtx = new ShardedDatabaseContext(Server.ServerStore, dbRecord);
                var ids = new[] { "users/0", "users/4", "users/1" };

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    for (int i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var shardNumber = shardedCtx.GetShardNumberFor(context, id);
                        Assert.Equal(i, shardNumber);
                    }
                }

                using (var session = src.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        session.Store(new User
                        {
                            Name = "User" + i
                        }, id);
                    }

                    session.SaveChanges();
                }

                var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
                WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var user = session.Load<User>(id);

                        Assert.NotNull(user);
                        Assert.Equal("User" + i, user.Name);
                    }
                }

                var etlDone = Sharding.Etl.WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Delete(ids[1]);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(ids[1]);

                    Assert.Null(user);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void RavenEtl_Sharded_Destination()
        {
            using (var src = Sharding.GetDocumentStore())
            using (var dest = Sharding.GetDocumentStore())
            {
                Server.ServerStore.Engine.Timeout.Disable = true;

                SetupRavenEtl(src, dest, "Users", script: null);

                var etlsDone = Sharding.Etl.WaitForEtlOnAllShards(src, (n, s) => s.LoadSuccesses > 0);
                var dbRecord = src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(src.Database)).Result;
                var shardedCtx = new ShardedDatabaseContext(Server.ServerStore, dbRecord);
                var ids = new[] { "users/0", "users/4", "users/1" };

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    for (int i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var shardNumber = shardedCtx.GetShardNumberFor(context, id);
                        Assert.Equal(i, shardNumber);
                    }
                }

                using (var session = src.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        session.Store(new User
                        {
                            Name = "User" + i
                        }, id);
                    }

                    session.SaveChanges();
                }

                var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
                WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var user = session.Load<User>(id);

                        Assert.NotNull(user);
                        Assert.Equal("User" + i, user.Name);
                    }
                }

                var etlDone = Sharding.Etl.WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Delete(ids[1]);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(ids[1]);

                    Assert.Null(user);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void RavenEtl_Loading_to_different_collections_with_load_document()
        {
            using (var src = Sharding.GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var etlsDone = Sharding.Etl.WaitForEtlOnAllShards(src, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupRavenEtl(src, dest, "users", @"
loadToUsers(this);
loadToPeople({Name: this.Name + ' ' + this.LastName });
loadToAddresses(load(this.AddressId));
");
                const int count = 10;

                using (var session = src.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        // make sure that user and address reside on the same shard
                        var userId = $"users/{i}";
                        var addressId = $"addresses/{i}/${userId}";

                        session.Store(new User
                        {
                            Age = i,
                            Name = "James",
                            LastName = "Smith",
                            AddressId = addressId
                        }, userId);

                        session.Store(new Address
                        {
                            City = "New York"
                        }, addressId);
                    }

                    session.SaveChanges();
                }

                var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
                WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    for (var i = 0; i < count; i++)
                    {
                        var user = session.Load<User>($"users/{i}");
                        Assert.NotNull(user);
                        Assert.Equal("James", user.Name);
                        Assert.Equal("Smith", user.LastName);

                        var metadata = session.Advanced.GetMetadataFor(user);
                        Assert.Equal("Users", metadata[Constants.Documents.Metadata.Collection]);

                        var person = session.Advanced.LoadStartingWith<Person>($"users/{i}/people/")[0];
                        Assert.NotNull(person);
                        Assert.Equal("James Smith", person.Name);

                        metadata = session.Advanced.GetMetadataFor(person);
                        Assert.Equal("People", metadata[Constants.Documents.Metadata.Collection]);

                        var address = session.Advanced.LoadStartingWith<Address>($"users/{i}/addresses/")[0];
                        Assert.NotNull(address);
                        Assert.Equal("New York", address.City);

                        metadata = session.Advanced.GetMetadataFor(address);
                        Assert.Equal("Addresses", metadata[Constants.Documents.Metadata.Collection]);
                    }
                }

                var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                var expectedCount = count * 3;
                Assert.Equal(expectedCount, stats.CountOfDocuments);

                var etlDone = Sharding.Etl.WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                using (var session = src.OpenSession())
                {
                    session.Delete("users/3");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/3");
                    Assert.Null(user);

                    var persons = session.Advanced.LoadStartingWith<Person>("users/3/people/");
                    Assert.Equal(0, persons.Length);

                    var addresses = session.Advanced.LoadStartingWith<Address>("users/3/addresses/");
                    Assert.Equal(0, addresses.Length);
                }

                expectedCount -= 3;

                stats = dest.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(expectedCount, stats.CountOfDocuments);
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void RavenEtl_Loading_to_different_collections()
        {
            using (var src = Sharding.GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var etlsDone = Sharding.Etl.WaitForEtlOnAllShards(src, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupRavenEtl(src, dest, "users", @"
loadToUsers(this);
loadToPeople({Name: this.Name + ' ' + this.LastName });
");
                const int count = 5;

                using (var session = src.OpenSession())
                {
                    for (int i = 0; i < count; i++)
                    {
                        session.Store(new User
                        {
                            Age = i,
                            Name = "James",
                            LastName = "Smith"
                        }, $"users/{i}");
                    }

                    session.SaveChanges();
                }

                var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
                WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1));
                using (var session = dest.OpenSession())
                {
                    for (var i = 0; i < count; i++)
                    {
                        var user = session.Load<User>($"users/{i}");
                        Assert.NotNull(user);
                        Assert.Equal("James", user.Name);
                        Assert.Equal("Smith", user.LastName);

                        var metadata = session.Advanced.GetMetadataFor(user);
                        Assert.Equal("Users", metadata[Constants.Documents.Metadata.Collection]);

                        var person = session.Advanced.LoadStartingWith<Person>($"users/{i}/people/")[0];
                        Assert.NotNull(person);
                        Assert.Equal("James Smith", person.Name);

                        metadata = session.Advanced.GetMetadataFor(person);
                        Assert.Equal("People", metadata[Constants.Documents.Metadata.Collection]);
                    }
                }

                var stats = dest.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(10, stats.CountOfDocuments);

                var etlDone = Sharding.Etl.WaitForEtl(src, (n, statistics) => statistics.LoadSuccesses != 0);

                using (var session = src.OpenSession())
                {
                    session.Delete("users/3");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromSeconds(30));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/3");
                    Assert.Null(user);

                    var persons = session.Advanced.LoadStartingWith<Person>("users/3/people/");
                    Assert.Equal(0, persons.Length);
                }

                stats = dest.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(8, stats.CountOfDocuments);
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Counters | RavenTestCategory.Sharding)]
        public void RavenEtl_Should_handle_counters()
        {
            using (var src = Sharding.GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                SetupRavenEtl(src, dest, "Users", script:
                    @"
var counters = this['@metadata']['@counters'];

this.Name = 'James';

// case 1 : doc id will be preserved

var doc = loadToUsers(this);

for (var i = 0; i < counters.length; i++) {
    doc.addCounter(loadCounter(counters[i]));
}

// case 2 : doc id will be generated on the destination side

var person = loadToPeople({ Name: this.Name + ' ' + this.LastName });

person.addCounter(loadCounter('down'));
"
);
                var etlDone = Sharding.Etl.WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    session.Store(new User()
                    {
                        Name = "Joe",
                        LastName = "Doe",
                    }, "users/1");

                    session.CountersFor("users/1").Increment("up", 20);
                    session.CountersFor("users/1").Increment("down", 10);

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                RavenDB_11157_Raven.AssertCounters(dest, new[]
                {
                    ("users/1", "up", 20L, false),
                    ("users/1", "down", 10, false),
                    ("users/1/people/", "down", 10, true)
                });

                string personId;

                using (var session = dest.OpenSession())
                {
                    personId = session.Advanced.LoadStartingWith<Person>("users/1/people/")[0].Id;
                }

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.CountersFor("users/1").Delete("up");
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/1");

                    var metadata = session.Advanced.GetMetadataFor(user);

                    Assert.True(metadata.ContainsKey(Constants.Documents.Metadata.Counters));

                    var counter = session.CountersFor("users/1").Get("up-etl");

                    Assert.Null(counter); // this counter was removed
                }

                RavenDB_11157_Raven.AssertCounters(dest, new[]
                {
                    ("users/1", "down", 10L, false),
                    ("users/1/people/", "down", 10, true)
                });

                etlDone.Reset();

                using (var session = src.OpenSession())
                {
                    session.Delete("users/1");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    Assert.Null(session.Load<User>("users/1"));

                    Assert.Null(session.CountersFor("users/1").Get("up"));
                    Assert.Null(session.CountersFor("users/1").Get("up"));

                    Assert.Empty(session.Advanced.LoadStartingWith<Person>("users/1/people/"));

                    Assert.Null(session.CountersFor(personId).Get("down-etl"));
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void CanDeleteEtl()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var name = "aaa";
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = name,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "S1",
                            Collections = {"Users"}
                        }
                    }
                };

                var result = Etl.AddEtl(store, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                });

                store.Maintenance.Send(new DeleteOngoingTaskOperation(result.TaskId, OngoingTaskType.RavenEtl));

                for (int i = 0; i < 3; i++)
                {
                    var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(name, OngoingTaskType.RavenEtl));
                    Assert.Null(ongoingTask);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void CanUpdateEtl()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var name = "aaa";
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = name,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "S1",
                            Collections = {"Users"}
                        },
                        new Transformation
                        {
                            Name = "S2",
                            Collections = {"Users"}
                        }
                    }
                };

                var result = Etl.AddEtl(store, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                });

                configuration.Transforms[0].Disabled = true;

                store.Maintenance.Send(new UpdateEtlOperation<RavenConnectionString>(result.TaskId, configuration));

                var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(name, OngoingTaskType.RavenEtl));

                Assert.Equal(OngoingTaskState.PartiallyEnabled, ongoingTask.TaskState);
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void CanDisableEtl()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var name = "aaa";
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = name,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "S1",
                            Collections = {"Users"}
                        }
                    }
                };

                var result = Etl.AddEtl(store, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                });

                var toggleResult = store.Maintenance.Send(new ToggleOngoingTaskStateOperation(result.TaskId, OngoingTaskType.RavenEtl, true));
                Assert.NotNull(toggleResult);
                Assert.True(toggleResult.RaftCommandIndex > 0);
                Assert.True(toggleResult.TaskId > 0);

                var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(name, OngoingTaskType.RavenEtl));
                Assert.Equal(OngoingTaskState.Disabled, ongoingTask.TaskState);
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void CanResetEtl()
        {
            using (var src = Sharding.GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var runs = 0;

                var etlDone = Sharding.Etl.WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                var resetDone = Sharding.Etl.WaitForEtl(src, (n, statistics) => ++runs >= 2);

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "allUsers",
                            Collections = {"Users"}
                        }
                    }
                };

                Etl.AddEtl(src, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                src.Maintenance.Send(new ResetEtlOperation("myConfiguration", "allUsers"));

                Assert.True(resetDone.Wait(TimeSpan.FromMinutes(1)));
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void CanResetEtl2()
        {
            using (var src = Sharding.GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                using (var session = src.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                var configuration = new RavenEtlConfiguration()
                {
                    ConnectionStringName = "test",
                    Name = "myConfiguration",
                    Transforms =
                    {
                        new Transformation()
                        {
                            Name = "allUsers",
                            Collections = {"Users"}
                        }
                    }
                };

                var mre = new ManualResetEvent(true);
                var mre2 = new ManualResetEvent(false);
                var etlDone = Sharding.Etl.WaitForEtl(src, (n, s) =>
                {
                    Assert.True(mre.WaitOne(TimeSpan.FromMinutes(1)));
                    mre.Reset();

                    mre2.Set();

                    return true;
                });

                Etl.AddEtl(src, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = dest.Urls,
                    Database = dest.Database,
                });

                var set = new HashSet<string>
                {
                    "asd"
                };

                for (int i = 0; i < 10; i++)
                {
                    Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)), $"blah at {i}");

                    mre.Set();

                    Assert.True(mre2.WaitOne(TimeSpan.FromMinutes(1)), $"oops at {i}");
                    mre2.Reset();

                    var t1 = src.Maintenance.SendAsync(new ResetEtlOperation("myConfiguration", "allUsers"));

                    for (int j = 0; j < 100; j++)
                    {
                        var t2 = src.Maintenance.Server.SendAsync(new UpdateUnusedDatabasesOperation(src.Database, set));
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void CanGetTaskInfo()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var name = "aaa";
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = name,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "S1",
                            Collections = {"Users"}
                        }
                    }
                };

                Etl.AddEtl(store, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind"
                });

                var ongoingTask = store.Maintenance.Send(new GetOngoingTaskInfoOperation(name, OngoingTaskType.RavenEtl));

                Assert.NotNull(ongoingTask);
                Assert.Equal(name, ongoingTask.TaskName);

                for (int i = 0; i < 3; i++)
                {
                    var singleShardInfo = store.Maintenance.ForShard(i).Send(new GetOngoingTaskInfoOperation(name, OngoingTaskType.RavenEtl));

                    Assert.NotNull(singleShardInfo);
                    Assert.Equal(name, singleShardInfo.TaskName);
                    Assert.Equal("A", singleShardInfo.ResponsibleNode.NodeTag);
                    Assert.Equal(OngoingTaskConnectionStatus.Active, singleShardInfo.TaskConnectionStatus);
                }
            }
        }

        [RequiresMsSqlFact]
        public void CanGetConnectionStringByName()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var ravenConnectionStrings = new List<RavenConnectionString>();
                var sqlConnectionStrings = new List<SqlConnectionString>();

                var ravenConnectionStr = new RavenConnectionString()
                {
                    Name = "RavenConnectionString",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                };
                var sqlConnectionStr = new SqlConnectionString
                {
                    Name = "SqlConnectionString",
                    ConnectionString = MsSqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}",
                    FactoryName = "System.Data.SqlClient"
                };
                ravenConnectionStrings.Add(ravenConnectionStr);
                sqlConnectionStrings.Add(sqlConnectionStr);

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionStr));
                Assert.NotNull(result1.RaftCommandIndex);
                var result2 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionStr));
                Assert.NotNull(result2.RaftCommandIndex);

                var result = store.Maintenance.Send(new GetConnectionStringsOperation(connectionStringName: sqlConnectionStr.Name, type: sqlConnectionStr.Type));
                Assert.True(result.SqlConnectionStrings.Count > 0);
                Assert.True(result.RavenConnectionStrings.Count == 0);

            }
        }

        [RequiresMsSqlFact]
        public void CanGetAllConnectionStrings()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var ravenConnectionStrings = new List<RavenConnectionString>();
                var sqlConnectionStrings = new List<SqlConnectionString>();
                for (var i = 0; i < 5; i++)
                {
                    var ravenConnectionStr = new RavenConnectionString()
                    {
                        Name = $"RavenConnectionString{i}",
                        TopologyDiscoveryUrls = new[] { $"http://127.0.0.1:808{i}" },
                        Database = "Northwind",
                    };
                    var sqlConnectionStr = new SqlConnectionString
                    {
                        Name = $"SqlConnectionString{i}",
                        ConnectionString = MsSqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}",
                        FactoryName = "System.Data.SqlClient"
                    };

                    ravenConnectionStrings.Add(ravenConnectionStr);
                    sqlConnectionStrings.Add(sqlConnectionStr);

                    var result1 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionStr));
                    Assert.NotNull(result1.RaftCommandIndex);
                    var result2 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionStr));
                    Assert.NotNull(result2.RaftCommandIndex);
                }

                var result = store.Maintenance.Send(new GetConnectionStringsOperation());
                Assert.NotNull(result.SqlConnectionStrings);
                Assert.NotNull(result.RavenConnectionStrings);

                for (var i = 0; i < 5; i++)
                {
                    result.SqlConnectionStrings.TryGetValue($"SqlConnectionString{i}", out var sql);
                    Assert.Equal(sql?.ConnectionString, sqlConnectionStrings[i].ConnectionString);

                    result.RavenConnectionStrings.TryGetValue($"RavenConnectionString{i}", out var raven);
                    Assert.Equal(raven?.TopologyDiscoveryUrls, ravenConnectionStrings[i].TopologyDiscoveryUrls);
                    Assert.Equal(raven?.Database, ravenConnectionStrings[i].Database);
                }
            }
        }

        [RequiresMsSqlFact]
        public void CanAddAndRemoveConnectionStrings()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var ravenConnectionString = new RavenConnectionString()
                {
                    Name = "RavenConnectionString",
                    TopologyDiscoveryUrls = new[] { "http://localhost:8080" },
                    Database = "Northwind",
                };
                var result0 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionString));
                Assert.NotNull(result0.RaftCommandIndex);

                var sqlConnectionString = new SqlConnectionString
                {
                    Name = "SqlConnectionString",
                    ConnectionString = MsSqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}",
                    FactoryName = "System.Data.SqlClient"
                };

                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result1.RaftCommandIndex);

                DatabaseRecord record;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.True(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.Equal(ravenConnectionString.Name, record.RavenConnectionStrings["RavenConnectionString"].Name);
                Assert.Equal(ravenConnectionString.TopologyDiscoveryUrls, record.RavenConnectionStrings["RavenConnectionString"].TopologyDiscoveryUrls);
                Assert.Equal(ravenConnectionString.Database, record.RavenConnectionStrings["RavenConnectionString"].Database);

                Assert.True(record.SqlConnectionStrings.ContainsKey("SqlConnectionString"));
                Assert.Equal(sqlConnectionString.Name, record.SqlConnectionStrings["SqlConnectionString"].Name);
                Assert.Equal(sqlConnectionString.ConnectionString, record.SqlConnectionStrings["SqlConnectionString"].ConnectionString);

                var result3 = store.Maintenance.Send(new RemoveConnectionStringOperation<RavenConnectionString>(ravenConnectionString));
                Assert.NotNull(result3.RaftCommandIndex);
                var result4 = store.Maintenance.Send(new RemoveConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result4.RaftCommandIndex);

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.False(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.False(record.SqlConnectionStrings.ContainsKey("SqlConnectionString"));

            }
        }

        [RequiresMsSqlFact]
        public void CanUpdateConnectionStrings()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var ravenConnectionString = new RavenConnectionString()
                {
                    Name = "RavenConnectionString",
                    TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8080" },
                    Database = "Northwind",
                };
                var result1 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionString));
                Assert.NotNull(result1.RaftCommandIndex);

                var sqlConnectionString = new SqlConnectionString
                {
                    Name = "SqlConnectionString",
                    ConnectionString = MsSqlConnectionString.Instance.VerifiedConnectionString.Value + $";Initial Catalog={store.Database}",
                    FactoryName = "System.Data.SqlClient"
                };

                var result2 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result2.RaftCommandIndex);

                //update url
                ravenConnectionString.TopologyDiscoveryUrls = new[] { "http://127.0.0.1:8081" };
                var result3 = store.Maintenance.Send(new PutConnectionStringOperation<RavenConnectionString>(ravenConnectionString));
                Assert.NotNull(result3.RaftCommandIndex);

                //update name : need to remove the old entry
                var result4 = store.Maintenance.Send(new RemoveConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result4.RaftCommandIndex);
                sqlConnectionString.Name = "New-Name";
                var result5 = store.Maintenance.Send(new PutConnectionStringOperation<SqlConnectionString>(sqlConnectionString));
                Assert.NotNull(result5.RaftCommandIndex);

                DatabaseRecord record;
                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                using (context.OpenReadTransaction())
                {
                    record = Server.ServerStore.Cluster.ReadDatabase(context, store.Database);
                }

                Assert.True(record.RavenConnectionStrings.ContainsKey("RavenConnectionString"));
                Assert.Equal("http://127.0.0.1:8081", record.RavenConnectionStrings["RavenConnectionString"].TopologyDiscoveryUrls.First());

                Assert.False(record.SqlConnectionStrings.ContainsKey("SqlConnectionString"));
                Assert.True(record.SqlConnectionStrings.ContainsKey("New-Name"));
                Assert.Equal(sqlConnectionString.ConnectionString, record.SqlConnectionStrings["New-Name"].ConnectionString);
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public async Task CanGetLastEtagPerDbFromProcessState()
        {
            using (var src = Sharding.GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                Server.ServerStore.Engine.Timeout.Disable = true;

                var connectionStringName = $"{src.Database}@{src.Urls.First()} to {dest.Database}@{dest.Urls.First()}";

                var config = new RavenEtlConfiguration
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>{ "Users" }
                        }
                    }
                };
                Etl.AddEtl(src, config,
                    new RavenConnectionString
                    {
                        Name = connectionStringName,
                        Database = dest.Database,
                        TopologyDiscoveryUrls = dest.Urls,
                    }
                );

                var etlsDone = Sharding.Etl.WaitForEtlOnAllShards(src, (n, s) => s.LoadSuccesses > 0);
                var dbRecord = src.Maintenance.Server.SendAsync(new GetDatabaseRecordOperation(src.Database)).Result;
                var shardedCtx = new ShardedDatabaseContext(Server.ServerStore, dbRecord);
                var ids = new[] { "users/0", "users/4", "users/1" };

                using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
                {
                    for (int i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var shardNumber = shardedCtx.GetShardNumberFor(context, id);
                        Assert.Equal(i, shardNumber);
                    }
                }

                using (var session = src.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        session.Store(new User
                        {
                            Name = "User" + i
                        }, id);
                    }

                    session.SaveChanges();
                }

                var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
                WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1));

                using (var session = dest.OpenSession())
                {
                    for (var i = 0; i < ids.Length; i++)
                    {
                        var id = ids[i];
                        var user = session.Load<User>(id);

                        Assert.NotNull(user);
                        Assert.Equal("User" + i, user.Name);
                    }
                }

                var tasks = Server.ServerStore.DatabasesLandlord.TryGetOrCreateShardedResourcesStore(src.Database);
                var dbs = new List<DocumentDatabase>();
                foreach (var task in tasks)
                {
                    dbs.Add(await task);
                }

                var state = EtlLoader.GetProcessState(config.Transforms, dbs.First(), config.Name);
                Assert.Equal(3, state.LastProcessedEtagPerDbId.Count);

                foreach (var db in dbs)
                {
                    Assert.True(state.LastProcessedEtagPerDbId.TryGetValue(db.DbBase64Id, out var etag));
                    Assert.Equal(1, etag);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding, MsSqlRequired = true)]
        public async Task SqlEtl_SimpleTransformation()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    SqlEtlTests.CreateRdbmsSchema(connectionString);

                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new Order
                        {
                            Lines = new List<OrderLine>
                            {
                                new OrderLine
                                {
                                    PricePerUnit = 3, Product = "Milk", Quantity = 3
                                },
                                new OrderLine
                                {
                                    PricePerUnit = 4, Product = "Beer", Quantity = 2
                                },
                            }
                        });

                        await session.SaveChangesAsync();
                    }

                    var etlDone = Sharding.Etl.WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                    SetupSqlEtl(store, connectionString, DefaultScript);

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    using (var con = new SqlConnection())
                    {
                        con.ConnectionString = connectionString;
                        con.Open();

                        using (var dbCommand = con.CreateCommand())
                        {
                            dbCommand.CommandText = " SELECT COUNT(*) FROM Orders";
                            Assert.Equal(1, dbCommand.ExecuteScalar());
                            dbCommand.CommandText = " SELECT COUNT(*) FROM OrderLines";
                            Assert.Equal(2, dbCommand.ExecuteScalar());
                        }
                    }
                }
            }
        }

        [RequiresMsSqlFact]
        public async Task SqlEtl_ReplicateMultipleBatches()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                using (SqlAwareTestBase.WithSqlDatabase(MigrationProvider.MsSQL, out var connectionString, out string schemaName, dataSet: null, includeData: false))
                {
                    SqlEtlTests.CreateRdbmsSchema(connectionString);
                    int testCount = 5000;

                    using (var bulkInsert = store.BulkInsert())
                    {
                        for (int i = 0; i < testCount; i++)
                        {
                            await bulkInsert.StoreAsync(new Order
                            {
                                Lines = new List<OrderLine>
                                {
                                    new OrderLine
                                    {
                                        PricePerUnit = 3, Product = "Milk", Quantity = 3
                                    },
                                    new OrderLine
                                    {
                                        PricePerUnit = 4, Product = "Beer", Quantity = 2
                                    },
                                }
                            });
                        }
                    }

                    var etlDone = Sharding.Etl.WaitForEtl(store, (n, s) => SqlEtlTests.GetOrdersCount(connectionString) == testCount);

                    SetupSqlEtl(store, connectionString, DefaultScript);

                    etlDone.Wait(TimeSpan.FromMinutes(5));

                    Assert.Equal(testCount, SqlEtlTests.GetOrdersCount(connectionString));
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public async Task OlapEtl_Local_Destination()
        {
            using (var store = Sharding.GetDocumentStore())
            {
                var baseline = new DateTime(2020, 1, 1);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i <= 10; i++)
                    {
                        var orderedAt = baseline.AddDays(i);
                        var o = new Query.Order
                        {
                            Id = $"orders/{i}",
                            OrderedAt = orderedAt,
                            RequireAt = orderedAt.AddDays(7),
                            Lines = new List<OrderLine>
                            {
                                new OrderLine
                                {
                                    Quantity = i * 10,
                                    PricePerUnit = (decimal)1.25,
                                }
                            }
                        };

                        await session.StoreAsync(o);
                    }

                    await session.SaveChangesAsync();
                }

                var etlsDone = Sharding.Etl.WaitForEtlOnAllShards(store, (n, statistics) => statistics.LoadSuccesses != 0);

                var script = @"
var o = {
    RequireAt : new Date(this.RequireAt)
    Total : 0
};

for (var j = 0; j < this.Lines.length; j++)
{
    var line = this.Lines[j];
    var p = line.Quantity * line.PricePerUnit;
    o.Total += p;
}

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

loadToOrders(partitionBy(key), o);
";

                var path = NewDataPath(forceCreateDir: true);
                SetupLocalOlapEtl(store, script, path);

                var waitHandles = etlsDone.Select(mre => mre.WaitHandle).ToArray();
                WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1));

                var files = Directory.GetFiles(path, searchPattern: AllFilesPattern, SearchOption.AllDirectories);
                Assert.Equal(3, files.Length);

                var expectedFields = new[] { "RequireAt", "Total", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };

                using (var fs = File.OpenRead(files[0]))
                using (var parquetReader = await ParquetReader.CreateAsync(fs))
                {
                    Assert.Equal(1, parquetReader.RowGroupCount);
                    Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                    using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                    foreach (var field in parquetReader.Schema.Fields)
                    {
                        Assert.True(field.Name.In(expectedFields));
                    }
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task OlapEtl_S3_Destination()
        {
            const string salesTableName = "Sales";
            const string partitionColumn = "order_date";

            var settings = Etl.GetS3Settings($"olap/tests/{nameof(ShardedEtlTests)}-{Guid.NewGuid()}");

            try
            {
                using (var store = Sharding.GetDocumentStore())
                {
                    var baseline = new DateTime(2020, 1, 1);

                    using (var session = store.OpenAsyncSession())
                    {
                        for (int i = 0; i < 31; i++)
                        {
                            var orderedAt = baseline.AddDays(i);
                            var lines = new List<OrderLine>();

                            for (int j = 1; j <= 5; j++)
                            {
                                lines.Add(new OrderLine
                                {
                                    Quantity = j * 10,
                                    PricePerUnit = i + j,
                                    Product = $"Products/{j}"
                                });
                            }

                            var o = new Query.Order
                            {
                                Id = $"orders/{i}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                Company = $"companies/{i}",
                                Lines = lines
                            };

                            await session.StoreAsync(o);
                        }

                        baseline = baseline.AddMonths(1);

                        for (int i = 0; i < 28; i++)
                        {
                            var orderedAt = baseline.AddDays(i);
                            var lines = new List<OrderLine>();

                            for (int j = 1; j <= 5; j++)
                            {
                                lines.Add(new OrderLine
                                {
                                    Quantity = j * 10,
                                    PricePerUnit = i + j,
                                    Product = $"Products/{j}"
                                });
                            }

                            var o = new Query.Order
                            {
                                Id = $"orders/{i + 31}",
                                OrderedAt = orderedAt,
                                RequireAt = orderedAt.AddDays(7),
                                Company = $"companies/{i}",
                                Lines = lines
                            };

                            await session.StoreAsync(o);
                        }

                        await session.SaveChangesAsync();
                    }

                    var etlDone = Sharding.Etl.WaitForEtlOnAllShards(store, (_, statistics) => statistics.LoadSuccesses != 0);

                    var script = @"
var orderData = {
    Company : this.Company,
    RequireAt : new Date(this.RequireAt),
    ItemsCount: this.Lines.length,
    TotalCost: 0
};

var orderDate = new Date(this.OrderedAt);
var year = orderDate.getFullYear();
var month = orderDate.getMonth();
var key = new Date(year, month);

for (var i = 0; i < this.Lines.length; i++) {
    var line = this.Lines[i];
    orderData.TotalCost += (line.PricePerUnit * line.Quantity);
    
    // load to 'sales' table

    loadToSales(partitionBy(['order_date', key]), {
        Qty: line.Quantity,
        Product: line.Product,
        Cost: line.PricePerUnit
    });
}

// load to 'orders' table
loadToOrders(partitionBy(['order_date', key]), orderData);
";


                    SetupS3OlapEtl(store, script, settings);

                    var waitHandles = etlDone.Select(mre => mre.WaitHandle).ToArray();
                    Assert.True(WaitHandle.WaitAll(waitHandles, TimeSpan.FromMinutes(1)));

                    using (var s3Client = new RavenAwsS3Client(settings, EtlTestBase.DefaultBackupConfiguration))
                    {
                        var prefix = $"{settings.RemoteFolderName}/Orders";
                        var forJanuary = $"{prefix}/{partitionColumn}=2020-01-01";
                        var forFebruary = $"{prefix}/{partitionColumn}=2020-02-01";

                        var januaryFiles = await s3Client.ListObjectsAsync(prefix: forJanuary, string.Empty, false);
                        Assert.Equal(3, januaryFiles.FileInfoDetails.Count);

                        var februaryFiles = await s3Client.ListObjectsAsync(prefix: forFebruary, string.Empty, false);
                        Assert.Equal(3, februaryFiles.FileInfoDetails.Count);

                        foreach (var files in new[] { januaryFiles, februaryFiles })
                        {
                            var paths = files.FileInfoDetails.Select(f => f.FullPath).ToList();
                            var record = await Sharding.GetShardingConfigurationAsync(store);
                            foreach (var shardNumber in record.Shards.Keys)
                            {
                                var shardName = $"{store.Database}${shardNumber}";
                                Assert.True(paths.Any(path => path.Contains(shardName)));
                            }
                        }

                        var fullPath = januaryFiles.FileInfoDetails[0].FullPath;
                        var blob = await s3Client.GetObjectAsync(fullPath);

                        await using var ms = new MemoryStream();
                        await blob.Data.CopyToAsync(ms);

                        using (var parquetReader = await ParquetReader.CreateAsync(ms))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);

                            var expectedFields = new[] { "Company", "RequireAt", "ItemsCount", "TotalCost", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };
                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));
                            }
                        }
                    }

                    using (var s3Client = new RavenAwsS3Client(settings, EtlTestBase.DefaultBackupConfiguration))
                    {
                        var prefix = $"{settings.RemoteFolderName}/{salesTableName}";

                        var forJanuary = $"{prefix}/{partitionColumn}=2020-01-01";
                        var forFebruary = $"{prefix}/{partitionColumn}=2020-02-01";

                        var januaryFiles = await s3Client.ListObjectsAsync(prefix: forJanuary, string.Empty, false);
                        Assert.Equal(3, januaryFiles.FileInfoDetails.Count);

                        var februaryFiles = await s3Client.ListObjectsAsync(prefix: forFebruary, string.Empty, false);
                        Assert.Equal(3, februaryFiles.FileInfoDetails.Count);

                        foreach (var files in new[] { januaryFiles, februaryFiles })
                        {
                            var paths = files.FileInfoDetails.Select(f => f.FullPath).ToList();

                            for (int i = 0; i < 3; i++)
                            {
                                var shardName = $"{store.Database}${i}";
                                Assert.True(paths.Any(path => path.Contains(shardName)));
                            }
                        }

                        var fullPath = februaryFiles.FileInfoDetails[1].FullPath;
                        var blob = await s3Client.GetObjectAsync(fullPath);

                        await using var ms = new MemoryStream();
                        await blob.Data.CopyToAsync(ms);

                        using (var parquetReader = await ParquetReader.CreateAsync(ms))
                        {
                            Assert.Equal(1, parquetReader.RowGroupCount);

                            var expectedFields = new[] { "Qty", "Product", "Cost", ParquetTransformedItems.DefaultIdColumn, ParquetTransformedItems.LastModifiedColumn };
                            Assert.Equal(expectedFields.Length, parquetReader.Schema.Fields.Count);

                            using var rowGroupReader = parquetReader.OpenRowGroupReader(0);
                            foreach (var field in parquetReader.Schema.Fields)
                            {
                                Assert.True(field.Name.In(expectedFields));
                            }
                        }
                    }
                }

            }
            finally
            {
                await S3Tests.DeleteObjects(settings, salesTableName);
            }
        }

        [RequiresElasticSearchRetryFact]
        public async Task ElasticEtl_SimpleScript()
        {
            using (var store = Sharding.GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                SetupElasticEtl(store, DefaultScript, new List<string>() { OrderIndexName, OrderLinesIndexName });
                var etlDone = Sharding.Etl.WaitForEtl(store, (n, statistics) => statistics.LoadSuccesses != 0);

                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Lines = new List<OrderLine>
                        {
                            new OrderLine
                            {
                                PricePerUnit = 3,
                                Product = "Cheese",
                                Quantity = 3
                            },
                            new OrderLine
                            {
                                PricePerUnit = 4,
                                Product = "Beer",
                                Quantity = 2
                            }
                        }
                    });
                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                await EnsureNonStaleElasticResultsAsync(client);

                var ordersCount = await client.CountAsync<object>(c => c.Indices(Indices.Index(OrderIndexName)));
                var orderLinesCount = await client.CountAsync<object>(c => c.Indices(OrderLinesIndexName));

                Assert.Equal(1, ordersCount.Count);
                Assert.Equal(2, orderLinesCount.Count);

                etlDone.Reset();

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1-A");

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                await EnsureNonStaleElasticResultsAsync(client);

                var ordersCountAfterDelete = await client.CountAsync<object>(c => c.Indices(Indices.Index(OrderIndexName)));
                var orderLinesCountAfterDelete = await client.CountAsync<object>(c => c.Indices(Indices.Index(OrderLinesIndexName)));

                Assert.Equal(0, ordersCountAfterDelete.Count);
                Assert.Equal(0, orderLinesCountAfterDelete.Count);
            }
        }

        [RequiresElasticSearchRetryFact]
        public async Task ElasticEtl_SimpleScriptWithManyDocuments()
        {
            using (var store = Sharding.GetDocumentStore())
            using (GetElasticClient(out var client))
            {
                var numberOfOrders = 100;
                var numberOfLinesPerOrder = 5;

                SetupElasticEtl(store, DefaultScript, new List<string>() { OrderIndexName, OrderLinesIndexName });

                for (int i = 0; i < numberOfOrders; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        Order order = new Order
                        {
                            Lines = new List<OrderLine>()
                        };

                        for (int j = 0; j < numberOfLinesPerOrder; j++)
                        {
                            order.Lines.Add(new OrderLine
                            {
                                PricePerUnit = j + 1,
                                Product = "foos/" + j,
                                Quantity = (i * j) % 10
                            });
                        }

                        session.Store(order, "orders/" + i);

                        session.SaveChanges();
                    }
                }

                var ordersCount = await WaitForValueAsync(async () =>
                {
                    await EnsureNonStaleElasticResultsAsync(client);
                    return (await client.CountAsync<object>(c => c.Indices(Indices.Index(OrderIndexName)))).Count;
                }, expectedVal: numberOfOrders, timeout: 60_000);

                Assert.True(ordersCount == numberOfOrders, await AddDebugInfoOnFailure(store, numberOfOrders, ordersCount));

                var orderLinesCount = await WaitForValueAsync(async () =>
                {
                    await EnsureNonStaleElasticResultsAsync(client);
                    return (await client.CountAsync<object>(c => c.Indices(Indices.Index(OrderLinesIndexName)))).Count;
                }, expectedVal: numberOfOrders * numberOfLinesPerOrder, timeout: 60_000);

                Assert.True(orderLinesCount == numberOfOrders * numberOfLinesPerOrder, await AddDebugInfoOnFailure(store, numberOfOrders * numberOfLinesPerOrder, orderLinesCount));

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < numberOfOrders; i++)
                    {
                        session.Delete("orders/" + i);
                    }

                    session.SaveChanges();
                }

                var ordersCountAfterDelete = await WaitForValueAsync(async () =>
                {
                    await EnsureNonStaleElasticResultsAsync(client);
                    return (await client.CountAsync<object>(c => c.Indices(Indices.Index(OrderIndexName)))).Count;
                }, expectedVal: 0, timeout: 60_000);

                Assert.True(ordersCountAfterDelete == 0, await AddDebugInfoOnFailure(store, 0, ordersCountAfterDelete));

                var orderLinesCountAfterDelete = await WaitForValueAsync(async () =>
                {
                    await EnsureNonStaleElasticResultsAsync(client);
                    return (await client.CountAsync<object>(c => c.Indices(Indices.Index(OrderLinesIndexName)))).Count;
                }, expectedVal: 0, timeout: 60_000);

                Assert.True(orderLinesCountAfterDelete == 0, await AddDebugInfoOnFailure(store, 0, orderLinesCountAfterDelete));
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public async Task RavenEtl_Resharding1()
        {
            using (var src = Sharding.GetDocumentStore())
            using (var dest = Sharding.GetDocumentStore())
            {
                SetupRavenEtl(src, dest, "Users", script: null);

                var etlDone = Sharding.Etl.WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                using (var session = src.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        session.Store(new User
                        {
                            Name = $"User-{i}"
                        }, $"users/{i}/$eu");
                    }

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        var user = session.Load<User>($"users/{i}/$eu");

                        Assert.NotNull(user);
                        Assert.Equal($"User-{i}", user.Name);
                    }
                }

                // move bucket of users/0/$eu

                etlDone = Sharding.Etl.WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                const string id = "users/0/$eu";
                var originalLocation = await Sharding.GetShardNumberForAsync(src, id);

                await Sharding.Resharding.MoveShardForId(src, id);
                var newLocation = await Sharding.GetShardNumberForAsync(src, id);

                Assert.NotEqual(originalLocation, newLocation);

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        var user = session.Load<User>($"users/{i}/$eu");

                        Assert.NotNull(user);
                        Assert.Equal($"User-{i}", user.Name);
                    }
                }

                etlDone = Sharding.Etl.WaitForEtl(src, (n, s) => s.LoadSuccesses > 0);

                // add new document to same bucket
                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "User-10"
                    }, "users/10/$eu");

                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>("users/10/$eu");

                    Assert.NotNull(user);
                    Assert.Equal("User-10", user.Name);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public async Task RavenEtl_Resharding2()
        {
            const string id = "users/1-A";
            using (var src = Sharding.GetDocumentStore())
            using (var dest = Sharding.GetDocumentStore())
            {
                SetupRavenEtl(src, dest, "Users", script: null);

                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende"
                    }, id);

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, id, user => user.Name == "ayende", timeout: 30_000));

                var etlDone = Sharding.Etl.WaitForEtl(src, (n, s) => s.LoadSuccesses >= 100);

                var originalLocation = await Sharding.GetShardNumberForAsync(src, id);
                string originalShard = ShardHelper.ToShardName(src.Database, originalLocation);

                // move bucket of users/1-A while writing

                var writes = Task.Run(() =>
                {
                    for (int i = 0; i < 100; i++)
                    {
                        using (var session = src.OpenSession())
                        {
                            session.Store(new User(), $"num-{i}${id}");
                            session.SaveChanges();
                        }
                    }
                });

                await Sharding.Resharding.MoveShardForId(src, id);
                await Sharding.Resharding.MoveShardForId(src, id);
                await Sharding.Resharding.MoveShardForId(src, id);
                await Sharding.Resharding.MoveShardForId(src, id);

                await writes;

                await AssertWaitForValueAsync(async () =>
                {
                    using (var session = src.OpenAsyncSession())
                    {
                        var q = await session.Query<User>().ToListAsync();
                        return q.Count;
                    }
                }, 101);

                var newLocation = await Sharding.GetShardNumberForAsync(src, id);
                string newShard = ShardHelper.ToShardName(src.Database, newLocation);

                using (var session = src.OpenSession(originalShard))
                {
                    var doc = session.Load<User>(id);
                    Assert.Null(doc);
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = src.OpenSession(originalShard))
                    {
                        var docId = $"num-{i}${id}";
                        var doc = session.Load<User>(docId);

                        Assert.Null(doc);
                    }

                    using (var session = src.OpenSession(newShard))
                    {
                        var docId = $"num-{i}${id}";
                        var doc = session.Load<User>(docId);

                        Assert.NotNull(doc);
                    }
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(90)));

                using (var session = dest.OpenSession())
                {
                    var user = session.Load<User>(id);

                    Assert.NotNull(user);
                    Assert.Equal("ayende", user.Name);
                }

                for (int i = 0; i < 100; i++)
                {
                    using (var session = dest.OpenSession())
                    {
                        var docId = $"num-{i}${id}";
                        var doc = session.Load<User>(docId);

                        Assert.NotNull(doc);
                    }
                }

                // add new document to same bucket
                using (var session = src.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "ayende2"
                    }, $"users/2-A/{id}");

                    session.SaveChanges();
                }

                Assert.True(WaitForDocument<User>(dest, $"users/2-A/{id}", user => user.Name == "ayende2", timeout: 30_000));
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public async Task RavenEtl_Resharding3()
        {
            var ids = new List<string>();
            using (var src = Sharding.GetDocumentStore())
            using (var dest = Sharding.GetDocumentStore())
            {
                SetupRavenEtl(src, dest, "Users", script: null);

                using (var session = src.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        var id = $"users/{i}";
                        ids.Add(id);
                        session.Store(new User(), id);

                    }

                    session.SaveChanges();
                }

                await AssertWaitForValueAsync(async () =>
                {
                    using (var session = dest.OpenAsyncSession())
                    {
                        var q = await session.Query<User>().ToListAsync();
                        return q.Count;
                    }
                }, ids.Count);

                // move buckets

                foreach (var id in ids)
                {
                    await Sharding.Resharding.MoveShardForId(src, id);
                }

                for (int i = 10; i < 100; i++)
                {
                    using (var session = src.OpenSession())
                    {
                        var id = $"users/{i}${ids[i % 10]}";
                        ids.Add(id);
                        session.Store(new User(), id);

                        session.SaveChanges();
                    }
                }

                var count = await WaitForValueAsync(async () =>
                {
                    using (var session = dest.OpenAsyncSession())
                    {
                        var q = await session.Query<User>().ToListAsync();
                        return q.Count;
                    }
                }, ids.Count, 60_000);

                Assert.True(count == ids.Count, await AddDebugInfoOnFailure(src, ids.Count, count));

                foreach (var id in ids)
                {
                    using (var session = dest.OpenSession())
                    {
                        var doc = session.Load<User>(id);
                        Assert.NotNull(doc);
                    }
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding, ElasticSearchRequired = true)]
        public void Sharded_Etl_DontAllowMentorNode()
        {
            using (var src = Sharding.GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                var error = Assert.ThrowsAny<RavenException>(() =>
                {
                    SetupRavenEtl(src, dest, "Users", script: null, mentor: "A");
                });

                Assert.Contains("Choosing a mentor node for an ongoing task is not supported in sharding", error.Message);

                error = Assert.ThrowsAny<RavenException>(() =>
                {
                    SetupElasticEtl(src, collections: new List<string>() { "Users" }, script: null, mentor: "A");
                });

                Assert.Contains("Choosing a mentor node for an ongoing task is not supported in sharding", error.Message);

                error = Assert.ThrowsAny<RavenException>(() =>
                {
                    SetupLocalOlapEtl(src, script: "some script", path: "./", mentor: "A");
                });

                Assert.Contains("Choosing a mentor node for an ongoing task is not supported in sharding", error.Message);

                error = Assert.ThrowsAny<RavenException>(() =>
                {
                    SetupSqlEtl(src, connectionString: "Test", script: null, mentor: "A");
                });

                Assert.Contains("Choosing a mentor node for an ongoing task is not supported in sharding", error.Message);
            }
        }

        [RavenFact(RavenTestCategory.Etl | RavenTestCategory.Sharding)]
        public void RavenDB_20437()
        {
            using (var src = GetDocumentStore())
            using (var dest = Sharding.GetDocumentStore())
            {
                var etlDone = Etl.WaitForEtlToComplete(src, (n, statistics) => statistics.LoadSuccesses != 0);

                SetupRavenEtl(src, dest, "Employees", @"
loadToPeople(this);
loadToAddresses(this.Address);
");
                const string id = "employees/1-A";

                using (var session = src.OpenSession())
                {
                    session.Store(new Employee
                    {
                        FirstName = "James",
                        Address = new Orders.Address
                        {
                            City = "New York"
                        }
                    }, id);
                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dest.OpenSession())
                {
                    var people = session.Advanced.LoadStartingWith<Employee>($"{id}/people/");
                    Assert.Equal(1, people.Length);

                    var person = people[0];
                    Assert.NotNull(person);
                    Assert.Equal("James", person.FirstName);

                    var metadata = session.Advanced.GetMetadataFor(person);
                    Assert.Equal("People", metadata[Constants.Documents.Metadata.Collection]);

                    var addresses = session.Advanced.LoadStartingWith<Address>($"{id}/addresses/");
                    Assert.Equal(1, addresses.Length);

                    var address = addresses[0];
                    Assert.NotNull(address);
                    Assert.Equal("New York", address.City);

                    metadata = session.Advanced.GetMetadataFor(address);
                    Assert.Equal("Addresses", metadata[Constants.Documents.Metadata.Collection]);
                }

                etlDone = Etl.WaitForEtlToComplete(src, (n, statistics) => statistics.LoadSuccesses != 0);

                using (var session = src.OpenSession())
                {
                    session.Delete(id);
                    session.SaveChanges();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(30)));

                using (var session = dest.OpenSession())
                {
                    var people = session.Advanced.LoadStartingWith<Employee>($"{id}/people/");
                    Assert.Equal(0, people.Length);

                    var addresses = session.Advanced.LoadStartingWith<Address>($"{id}/addresses/");
                    Assert.Equal(0, addresses.Length);
                }
            }
        }

        private AddEtlOperationResult SetupRavenEtl(IDocumentStore src, IDocumentStore dst, string collection, string script, bool applyToAllDocuments = false, bool disabled = false, string mentor = null)
        {
            var connectionStringName = $"{src.Database}@{src.Urls.First()} to {dst.Database}@{dst.Urls.First()}";

            return Etl.AddEtl(src, new RavenEtlConfiguration
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>{ collection },
                            Script = script,
                            ApplyToAllDocuments = applyToAllDocuments,
                            Disabled = disabled
                        }
                    },
                MentorNode = mentor,
            },
                new RavenConnectionString
                {
                    Name = connectionStringName,
                    Database = dst.Database,
                    TopologyDiscoveryUrls = dst.Urls,
                }
            );
        }

        private AddEtlOperationResult SetupLocalOlapEtl(IDocumentStore store, string script, string path, string name = "olap-test", string frequency = null,
            string transformationName = null, string mentor = null)
        {
            var connectionStringName = $"{store.Database} to local";
            var configuration = new OlapEtlConfiguration
            {
                Name = name,
                ConnectionStringName = connectionStringName,
                RunFrequency = frequency ?? DefaultFrequency,
                Transforms =
                {
                    new Transformation
                    {
                        Name = transformationName ?? "MonthlyOrders",
                        Collections = new List<string> {"Orders"},
                        Script = script
                    }
                },
                MentorNode = mentor
            };

            var connectionString = new OlapConnectionString
            {
                Name = connectionStringName,
                LocalSettings = new LocalSettings
                {
                    FolderPath = path
                }
            };

            return Etl.AddEtl(store, configuration, connectionString);
        }

        private AddEtlOperationResult SetupS3OlapEtl(IDocumentStore store, string script, S3Settings settings, string customPartitionValue = null, string transformationName = null)
        {
            var connectionStringName = $"{store.Database} to S3";

            var configuration = new OlapEtlConfiguration
            {
                Name = "olap-s3-test",
                ConnectionStringName = connectionStringName,
                RunFrequency = DefaultFrequency,
                CustomPartitionValue = customPartitionValue,
                Transforms =
                {
                    new Transformation
                    {
                        Name = transformationName ?? "MonthlyOrders",
                        Collections = new List<string> {"Orders"},
                        Script = script
                    }
                }
            };
            return Etl.AddEtl(store, configuration, new OlapConnectionString
            {
                Name = connectionStringName,
                S3Settings = settings
            });
        }

        private AddEtlOperationResult SetupSqlEtl(IDocumentStore store, string connectionString, string script, bool insertOnly = false, List<string> collections = null, string mentor = null)
        {
            var connectionStringName = $"{store.Database}@{store.Urls.First()} to SQL DB";

            var configuration = new SqlEtlConfiguration
            {
                Name = connectionStringName,
                ConnectionStringName = connectionStringName,
                SqlTables =
                {
                    new SqlEtlTable {TableName = "Orders", DocumentIdColumn = "Id", InsertOnlyMode = insertOnly},
                    new SqlEtlTable {TableName = "OrderLines", DocumentIdColumn = "OrderId", InsertOnlyMode = insertOnly},
                },
                Transforms =
                {
                    new Transformation()
                    {
                        Name = "OrdersAndLines",
                        Collections = collections ?? new List<string> {"Orders"},
                        Script = script
                    }
                },
                MentorNode = mentor
            };

            return Etl.AddEtl(store, configuration, new SqlConnectionString
            {
                Name = connectionStringName,
                ConnectionString = connectionString,
                FactoryName = "System.Data.SqlClient"
            });
        }

        private void SetupElasticEtl(IDocumentStore store, string script, IEnumerable<string> collections, string mentor = null)
        {
            var connectionStringName = $"{store.Database}@{store.Urls.First()} to ELASTIC";

            Etl.AddEtl(store,
                new ElasticSearchEtlConfiguration()
                {
                    Name = connectionStringName,
                    ConnectionStringName = connectionStringName,
                    ElasticIndexes =
                    {
                        new ElasticSearchIndex
                        {
                            IndexName = "Orders",
                            DocumentIdProperty = "Id"
                        },
                        new ElasticSearchIndex
                        {
                            IndexName = "OrderLines",
                            DocumentIdProperty = "OrderId"
                        },
                        new ElasticSearchIndex
                        {
                            IndexName = "Users",
                            DocumentIdProperty = "UserId"
                        }
                    },
                    MentorNode = mentor,
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = $"ETL : {connectionStringName}",
                            Collections = new List<string>(collections),
                            Script = script
                        }
                    }
                },

                new ElasticSearchConnectionString
                {
                    Name = connectionStringName,
                    Nodes = ElasticSearchTestNodes.Instance.VerifiedNodes.Value
                });
        }

        private IDisposable GetElasticClient(out ElasticsearchClient client)
        {
            var localClient = client = ElasticSearchHelper.CreateClient(new ElasticSearchConnectionString { Nodes = ElasticSearchTestNodes.Instance.VerifiedNodes.Value });

            AsyncHelpers.RunSync(() => CleanupIndexes(localClient));

            return new DisposableAction(() =>
            {
                AsyncHelpers.RunSync(() => CleanupIndexes(localClient));
            });
        }

        private static async Task CleanupIndexes(ElasticsearchClient client)
        {
            var indices = await client.Indices.GetAsync(new GetIndexRequestDescriptor(Indices.All));
            foreach (var indexName in indices.Indices.Keys)
            {
                var response = await client.Indices.DeleteAsync(Indices.Index(indexName));

                if (response.IsValidResponse)
                    continue;

                response.TryGetOriginalException(out var originalException);
                response.TryGetElasticsearchServerError(out var elasticsearchServerError);
                throw new InvalidOperationException($"Failed to delete elasticsearch index '{indexName}'. Elasticsearch Server Error: {elasticsearchServerError.Error}",
                    originalException);
            }
        }

        private static Task EnsureNonStaleElasticResultsAsync(ElasticsearchClient client)
        {
            return client.Indices.RefreshAsync(new RefreshRequest(Indices.All));
        }

        private async Task<string> AddDebugInfoOnFailure(IDocumentStore store, int expected, long actual)
        {
            var sb = new StringBuilder();
            sb.AppendLine($"not all docs reached destination, expected {expected} docs but got {actual}");

            var performanceStats = await Etl.GetEtlDebugInfo(store.Database, timeout: TimeSpan.FromSeconds(60), databaseMode: RavenDatabaseMode.Sharded);
            sb.AppendLine(performanceStats);

            return sb.ToString();
        }

    }
}
