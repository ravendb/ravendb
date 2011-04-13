//-----------------------------------------------------------------------
// <copyright file="Basic.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Diagnostics;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Http;
using Raven.Server;
using Xunit;
using Raven.Client.Extensions;
using System.Linq;

namespace Raven.Tests.Bugs.MultiTenancy
{
    public class Basic : RemoteClientTest, IDisposable
    {
        protected RavenDbServer GetNewServer(int port)
        {
            return new RavenDbServer(new RavenConfiguration
                {
                    Port = port,
                    RunInMemory = true,
                    DataDirectory = "Data",
                    AnonymousUserAccessMode = AnonymousUserAccessMode.All
                });
        }

        [Fact]
        public void CanCreateDatabaseUsingExtensionMethod()
        {
            using (GetNewServer(8080))
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080"
            }.Initialize())
            {
                store.DatabaseCommands.EnsureDatabaseExists("Northwind");
                
                string userId;

                using (var s = store.OpenSession("Northwind"))
                {
                    var entity = new User
                    {
                        Name = "First Mutlti Tenant Bank",
                    };
                    s.Store(entity);
                    userId = entity.Id;
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.Null(s.Load<User>(userId));
                }

                using (var s = store.OpenSession("Northwind"))
                {
                    Assert.NotNull(s.Load<User>(userId));
                }
            }
        }

        [Fact]
        public void CanQueryTenantDatabase()
        {
            using (GetNewServer(8080))
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080"
            }.Initialize())
            {
                store.DatabaseCommands.EnsureDatabaseExists("Northwind");

                using (var s = store.OpenSession("Northwind"))
                {
                    var entity = new User
                    {
                        Name = "Hello",
                    };
                    s.Store(entity);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession("Northwind"))
                {
                    Assert.NotEmpty(s.Query<User>().Where(x => x.Name == "Hello"));
                }
            }
        }


        [Fact]
        public void CanQueryDefaultDatabaseQuickly()
        {
            using (GetNewServer(8080))
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080"
            }.Initialize())
            {
                store.DatabaseCommands.EnsureDatabaseExists("Northwind");

                using (var s = store.OpenSession("Northwind"))
                {
                    var entity = new User
                    {
                        Name = "Hello",
                    };
                    s.Store(entity);
                    s.SaveChanges();
                }

                var sp = Stopwatch.StartNew();
                using (var s = store.OpenSession())
                {
                    Assert.Empty(s.Query<User>().Where(x => x.Name == "Hello"));
                }
                Assert.True(TimeSpan.FromSeconds(5) > sp.Elapsed);
            }
        }


        [Fact]
        public void OpenSessionUsesSpecifiedDefaultDatabase()
        {
            using (GetNewServer(8080))
            using (var store = new DocumentStore
            {
                Url = "http://localhost:8080",
                DefaultDatabase = "Northwind"
            }.Initialize())
            {
                store.DatabaseCommands.EnsureDatabaseExists("Northwind");

                string userId;

                using (var s = store.OpenSession("Northwind"))
                {
                    var entity = new User
                    {
                        Name = "First Mutlti Tenant Bank",
                    };
                    s.Store(entity);
                    userId = entity.Id;
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.NotNull(s.Load<User>(userId));
                }
            }
        }

        [Fact]
        public void CanUseMultipleDatabases()
        {
            using(GetNewServer(8080))
            using(var store = new DocumentStore
            {
                Url = "http://localhost:8080"
            }.Initialize())
            {
                using(var s = store.OpenSession())
                {
                    s.Store(new DatabaseDocument
                    {
                        Id = "Raven/Databases/Northwind",
                        Settings =
                            {
                                {"Raven/RunInMemory", "true"},
                                {"Raven/DataDir", "Northwind"}
                            }
                    });

                    s.SaveChanges();
                }

                string userId;

                using(var s = store.OpenSession("Northwind"))
                {
                    var entity = new User
                    {
                        Name = "First Mutlti Tenant Bank",
                    };
                    s.Store(entity);
                    userId = entity.Id;
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.Null(s.Load<User>(userId));
                }

                using (var s = store.OpenSession("Northwind"))
                {
                    Assert.NotNull(s.Load<User>(userId));
                }
            }
        }

        public void Dispose()
        {
            IOExtensions.DeleteDirectory("Data");
            IOExtensions.DeleteDirectory("NHibernate");
        }
    }
}