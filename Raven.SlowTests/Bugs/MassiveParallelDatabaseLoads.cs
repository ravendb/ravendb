using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;
using Xunit.Extensions;

namespace Raven.SlowTests.Bugs
{
    public class MassiveParallelDatabaseLoads:RavenTestBase
    {
        public class User
        {
            public string Name { get; set; }
        }

        public class UserIndex1 : AbstractIndexCreationTask<User>
        {
            
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task TestMassiveParallelDatabaseLoads(string storageEngine)
        {
            using (var server = GetNewServer())
            {
                var defaultStore = server.DocumentStore;
                var userRJO = RavenJObject.FromObject(new User
                {
                    Name = "Abe"
                });
                for (var i = 0; i < 120; i++)
                {
                    var dbName = "Database" + i;
                    var databaseDocument = MultiDatabase.CreateDatabaseDocument(dbName);
                    databaseDocument.Settings["Raven/StorageTypeName"] = storageEngine;

                    databaseDocument.Settings.Add("Raven/Tenants/MaxIdleTimeForTenantDatabase", "20");
                    await defaultStore.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(
                        databaseDocument).ConfigureAwait(false);
                    using (var curStore = new DocumentStore()
                    {
                        Url = GetServerUrl(false, server.SystemDatabase.ServerUrl),
                        Conventions = new DocumentConvention()
                        {
                            FailoverBehavior = FailoverBehavior.FailImmediately
                        },
                        DefaultDatabase = dbName
                    })
                    {
                        curStore.Initialize();
                       
                        using (var bi = curStore.BulkInsert())
                        {
                            for (var j = 0; j < 5; j++)
                            {
                                bi.Store(new User()
                                {
                                    Name = "UserNumber" + j
                                });
                            }
                        }
                    }

                    await defaultStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                       defaultStore.Url + "/admin/databases-toggle-disable?id=" + dbName + "&isSettingDisabled=true",
                       HttpMethod.Post,
                       null,
                       defaultStore.Conventions,
                       null)).ExecuteRequestAsync().ConfigureAwait(false);
                }

                Parallel.For(0, 120, new ParallelOptions()
                {
                    MaxDegreeOfParallelism = 20
                },i =>
                {
                    var dbName = "Database" + i;
                    defaultStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                        defaultStore.Url + "/admin/databases-toggle-disable?id=" + dbName + "&isSettingDisabled=false",
                        HttpMethod.Post,
                        null,
                        defaultStore.Conventions,
                        null)).ExecuteRequest();

                    using (var store = NewDocumentStore(databaseName: dbName, seedData: new[] {new string[] {}}, indexes: new AbstractIndexCreationTask[]
                    {

                    }))
                    {
                        store.DatabaseCommands.Put("users/1", null, userRJO, null);
                    }
                });
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public async Task TestMassiveParallelDatabaseLoadsWithALotOfIndexesPerDatabase(string storageEngine)
        {
            var indexes = new List<IndexDefinition>();

            for (var i = 0; i < 120; i++)
            {
                indexes.Add(new IndexDefinition()
                {
                    Name = "IndexNO" + i,
                    Map = "from user in docs.Users select new {user.Name}",
                });
            }
            using (var server = GetNewServer())
            {
                var defaultStore = server.DocumentStore;
                var userRJO = RavenJObject.FromObject(new User
                {
                    Name = "Abe"
                });
                for (var i = 0; i < 120; i++)
                {
                    var dbName = "Database" + i;
                    var databaseDocument = MultiDatabase.CreateDatabaseDocument(dbName);
                    databaseDocument.Settings["Raven/StorageTypeName"] = storageEngine;

                    databaseDocument.Settings.Add("Raven/Tenants/MaxIdleTimeForTenantDatabase", "20");
                    await defaultStore.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(
                        databaseDocument).ConfigureAwait(false);
                    using (var curStore = new DocumentStore()
                    {
                        Url = GetServerUrl(false, server.SystemDatabase.ServerUrl),
                        Conventions = new DocumentConvention()
                        {
                            FailoverBehavior = FailoverBehavior.FailImmediately
                        },
                        DefaultDatabase = dbName
                    })
                    {
                        curStore.Initialize();

                        foreach (var index in indexes)
                        {
                            curStore.DatabaseCommands.PutIndex(index.Name, index);
                        }

                        using (var bi = curStore.BulkInsert())
                        {
                            for (var j = 0; j < 5; j++)
                            {
                                bi.Store(new User()
                                {
                                    Name = "UserNumber" + j
                                });
                            }
                        }
                    }

                    await defaultStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                       defaultStore.Url + "/admin/databases-toggle-disable?id=" + dbName + "&isSettingDisabled=true",
                       HttpMethod.Post,
                       null,
                       defaultStore.Conventions,
                       null)).ExecuteRequestAsync().ConfigureAwait(false);
                }

                Parallel.For(0, 120, new ParallelOptions()
                {
                    MaxDegreeOfParallelism = 20
                }, i =>
                {
                    var dbName = "Database" + i;
                    defaultStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                        defaultStore.Url + "/admin/databases-toggle-disable?id=" + dbName + "&isSettingDisabled=false",
                        HttpMethod.Post,
                        null,
                        defaultStore.Conventions,
                        null)).ExecuteRequest();

                    using (var store = NewDocumentStore(databaseName: dbName, seedData: new[] { new string[] { } }, indexes: new AbstractIndexCreationTask[]
                    {

                    }))
                    {
                        store.DatabaseCommands.Put("users/1", null, userRJO, null);
                    }
                });
            }
        }

        [Theory(Skip = "This test should be moved to manual tests suite, that will verify the system's behaviour")]
        [PropertyData("Storages")]
        public async Task TestMassiveParallelDatabaseTakedownsAndLoads(string storageEngine)
        {
            using (var server = GetNewServer())
            {
                var defaultStore = server.DocumentStore;
                var userRJO = RavenJObject.FromObject(new User
                {
                    Name = "Abe"
                });
                for (var i = 0; i < 120; i++)
                {
                    var dbName = "Database" + i;
                    var databaseDocument = MultiDatabase.CreateDatabaseDocument(dbName);
                    databaseDocument.Settings["Raven/StorageTypeName"] = storageEngine;

                    databaseDocument.Settings.Add("Raven/Tenants/MaxIdleTimeForTenantDatabase", "20");
                    await defaultStore.AsyncDatabaseCommands.GlobalAdmin.CreateDatabaseAsync(
                        databaseDocument).ConfigureAwait(false);
                    using (var curStore = new DocumentStore()
                    {
                        Url = GetServerUrl(false, server.SystemDatabase.ServerUrl),
                        Conventions = new DocumentConvention()
                        {
                            FailoverBehavior = FailoverBehavior.FailImmediately
                        },
                        DefaultDatabase = dbName
                    })
                    {
                        curStore.Initialize();

                        using (var bi = curStore.BulkInsert())
                        {
                            for (var j = 0; j < 5; j++)
                            {
                                bi.Store(new User()
                                {
                                    Name = "UserNumber" + j
                                });
                            }
                        }
                    }
                }

                
                var threadCountWhenAllDatabasesActive = GetThreadsCount();

                var sp = Stopwatch.StartNew();

                Parallel.For(0, 120, new ParallelOptions()
                {
                    MaxDegreeOfParallelism = 20
                }, i =>
                {
                    var dbName = "Database" + i;
                    defaultStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                        defaultStore.Url + "/admin/databases-toggle-disable?id=" + dbName + "&isSettingDisabled=true",
                        HttpMethod.Post,
                        null,
                        defaultStore.Conventions,
                        null)).ExecuteRequest();
                });

                Parallel.For(0, 120, new ParallelOptions()
                {
                    MaxDegreeOfParallelism = 20
                }, i =>
                {
                    while (sp.ElapsedMilliseconds<60000)
                    {
                        try
                        {
                            var dbName = "Database" + i;
                            defaultStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                                defaultStore.Url + "/admin/databases-toggle-disable?id=" + dbName + "&isSettingDisabled=false",
                                HttpMethod.Post,
                                null,
                                defaultStore.Conventions,
                                null)).ExecuteRequest();

                            using (var store = NewDocumentStore(databaseName: dbName, seedData: new[] {new string[] {}}, indexes: new AbstractIndexCreationTask[]
                            {

                            }))
                            {
                                store.DatabaseCommands.Put("users/1", null, userRJO, null);
                            }

                            defaultStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                                defaultStore.Url + "/admin/databases-toggle-disable?id=" + dbName + "&isSettingDisabled=true",
                                HttpMethod.Post,
                                null,
                                defaultStore.Conventions,
                                null)).ExecuteRequest();
                        }
                        catch (Exception)
                        {
                        }
                    }
                });

                Parallel.For(0, 120, new ParallelOptions()
                {
                    MaxDegreeOfParallelism = 20
                }, i =>
                {
                    var dbName = "Database" + i;
                    defaultStore.JsonRequestFactory.CreateHttpJsonRequest(new CreateHttpJsonRequestParams(null,
                        defaultStore.Url + "/admin/databases-toggle-disable?id=" + dbName + "&isSettingDisabled=false",
                        HttpMethod.Post,
                        null,
                        defaultStore.Conventions,
                        null)).ExecuteRequest();

                    using (var curStore = new DocumentStore()
                    {
                        Url = GetServerUrl(false, server.SystemDatabase.ServerUrl),
                        Conventions = new DocumentConvention()
                        {
                            FailoverBehavior = FailoverBehavior.FailImmediately
                        },
                        DefaultDatabase = dbName
                    })
                    {
                        curStore.Initialize();

                        using (var bi = curStore.BulkInsert())
                        {
                            for (var j = 0; j < 5; j++)
                            {
                                bi.Store(new User()
                                {
                                    Name = "UserNumber" + j
                                });
                            }
                        }
                    }
                });
                
                var threadCountAfterStress = GetThreadsCount();
                
                Assert.InRange(Math.Abs(threadCountAfterStress - threadCountWhenAllDatabasesActive), 0, threadCountWhenAllDatabasesActive / 5);
            }
        }

        private int GetThreadsCount()
        {
            return Process.GetCurrentProcess().Threads.Count;
        }
        private float GetCpuUsage()
        {
            PerformanceCounter cpuCounter;
            cpuCounter = new PerformanceCounter();
            cpuCounter.CategoryName = "Processor";
            cpuCounter.CounterName = "% Processor Time";
            cpuCounter.InstanceName = "_Total";
            var values = new float[10];

            for (int i = 0; i < values.Length; i++)
            {
                values[i] = cpuCounter.NextValue();
            }
            return values.Average();
        }
    }
}
