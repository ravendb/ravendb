//-----------------------------------------------------------------------
// <copyright file="Basic.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System.Diagnostics;
using FastTests;
using System.Linq;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.MultiTenancy
{
    public class Basic : RavenTestBase
    {
        public Basic(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanCreateDatabaseUsingExtensionMethod()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                var doc = new DatabaseRecord("Northwind");
                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

                string userId;

                using (var s = store.OpenSession("Northwind"))
                {
                    var entity = new User
                    {
                        Name = "First Multitenant Bank",
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
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                var doc = new DatabaseRecord("Northwind");
                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

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
        public void CanQueryDefaultDatabase()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                var doc = new DatabaseRecord("Northwind");
                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

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
            }
        }


        [Fact]
        public void OpenSessionUsesSpecifiedDefaultDatabase()
        {
            DoNotReuseServer();
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Database = "Northwind";
                }
            }))
            {
                Assert.Equal("Northwind", store.Database);
                var doc = new DatabaseRecord("Northwind");
                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

                string userId;

                using (var s = store.OpenSession("Northwind"))
                {
                    var entity = new User
                    {
                        Name = "First Multitenant Bank",
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
            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                var doc = new DatabaseRecord("Northwind");
                store.Maintenance.Server.Send(new CreateDatabaseOperation(doc));

                string userId;

                using (var session = store.OpenSession("Northwind"))
                {
                    var entity = new User
                    {
                        Name = "First Multitenant Bank",
                    };
                    session.Store(entity);
                    userId = entity.Id;
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<User>(userId));
                }

                using (var session = store.OpenSession("Northwind"))
                {
                    Assert.NotNull(session.Load<User>(userId));
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }
    }
}
