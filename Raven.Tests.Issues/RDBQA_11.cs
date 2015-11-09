// -----------------------------------------------------------------------
//  <copyright file="RDBQA_11.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

using Raven.Abstractions;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Client;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Files;
using Raven.Smuggler.Database.Remote;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RDBQA_11 : RavenTest
    {
        private class Product
        {
            public int Id { get; set; }
        }

        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Core.ActiveBundlesStringValue = "DocumentExpiration";
        }

        [Fact, Trait("Category", "Smuggler")]
        public void SmugglerWithoutExcludeExpiredDocumentsShouldWork()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    Initialize(store);

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(),
                        new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Database = store.DefaultDatabase,
                            Url = store.Url
                        }),
                        new DatabaseSmugglerFileDestination(path));

                    smuggler.Execute();
                }

                using (var store = NewRemoteDocumentStore())
                {
                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions(),
                        new DatabaseSmugglerFileSource(path),
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = store.Url,
                            Database = store.DefaultDatabase
                        }));

                    smuggler.Execute();

                    using (var session = store.OpenSession())
                    {
                        var product1 = session.Load<Product>(1);
                        var product2 = session.Load<Product>(2);
                        var product3 = session.Load<Product>(3);

                        Assert.NotNull(product1);
                        Assert.Null(product2);
                        Assert.NotNull(product3);
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(path);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void SmugglerWithExcludeExpiredDocumentsShouldWork1()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    Initialize(store);

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions { ShouldExcludeExpired = true },
                        new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Database = store.DefaultDatabase,
                            Url = store.Url
                        }),
                        new DatabaseSmugglerFileDestination(path));

                    smuggler.Execute();
                }

                using (var store = NewRemoteDocumentStore())
                {
                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions { ShouldExcludeExpired = true },
                        new DatabaseSmugglerFileSource(path),
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = store.Url,
                            Database = store.DefaultDatabase
                        }));

                    smuggler.Execute();

                    using (var session = store.OpenSession())
                    {
                        var product1 = session.Load<Product>(1);
                        var product2 = session.Load<Product>(2);
                        var product3 = session.Load<Product>(3);

                        Assert.NotNull(product1);
                        Assert.Null(product2);
                        Assert.NotNull(product3);
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(path);
            }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void SmugglerWithExcludeExpiredDocumentsShouldWork2()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    Initialize(store);

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions { ShouldExcludeExpired = true },
                        new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Database = store.DefaultDatabase,
                            Url = store.Url
                        }),
                        new DatabaseSmugglerFileDestination(path));

                    smuggler.Execute();
                }

                using (var store = NewRemoteDocumentStore())
                {
                    SystemTime.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                    var smuggler = new DatabaseSmuggler(
                        new DatabaseSmugglerOptions { ShouldExcludeExpired = true },
                        new DatabaseSmugglerFileSource(path),
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = store.Url,
                            Database = store.DefaultDatabase
                        }));

                    smuggler.Execute();

                    using (var session = store.OpenSession())
                    {
                        var product1 = session.Load<Product>(1);
                        var product2 = session.Load<Product>(2);
                        var product3 = session.Load<Product>(3);

                        Assert.NotNull(product1);
                        Assert.Null(product2);
                        Assert.Null(product3);
                    }
                }
            }
            finally
            {
                IOExtensions.DeleteDirectory(path);
            }
        }

        private static void Initialize(IDocumentStore store)
        {
            var product1 = new Product();
            var product2 = new Product();
            var product3 = new Product();

            var future = SystemTime.UtcNow.AddMinutes(5);
            var past = SystemTime.UtcNow.AddMinutes(-5);
            using (var session = store.OpenSession())
            {
                session.Store(product1);
                session.Store(product2);
                session.Store(product3);

                session.Advanced.GetMetadataFor(product2)["Raven-Expiration-Date"] = new RavenJValue(past);
                session.Advanced.GetMetadataFor(product3)["Raven-Expiration-Date"] = new RavenJValue(future);

                session.SaveChanges();
            }
        }
    }
}
