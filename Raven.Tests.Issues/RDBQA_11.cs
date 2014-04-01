// -----------------------------------------------------------------------
//  <copyright file="RDBQA_11.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Tests.Common;

namespace Raven.Tests.Issues
{
    using System;
    using System.IO;

    using Raven.Abstractions;
    using Raven.Abstractions.Data;
    using Raven.Abstractions.Smuggler;
    using Raven.Client;
    using Raven.Database.Extensions;
    using Raven.Json.Linq;
    using Raven.Smuggler;

    using Xunit;

    public class RDBQA_11 : RavenTest
    {
        private class Product
        {
            public int Id { get; set; }
        }

        protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
        {
            configuration.Settings["Raven/ActiveBundles"] = "DocumentExpiration";
        }

        [Fact]
        public void SmugglerWithoutExcludeExpiredDocumentsShouldWork()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    Initialize(store);

                    var smuggler = new SmugglerApi();

					smuggler.ExportData(new SmugglerExportOptions { ToFile = path, From = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = store.DefaultDatabase } }, new SmugglerOptions()).Wait(TimeSpan.FromSeconds(15));
                }

                using (var store = NewRemoteDocumentStore())
                {
                    var smuggler = new SmugglerApi();

					smuggler.ImportData(new SmugglerImportOptions { FromFile = path, To = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = store.DefaultDatabase } }, new SmugglerOptions()).Wait(TimeSpan.FromSeconds(15));

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

        [Fact]
        public void SmugglerWithExcludeExpiredDocumentsShouldWork1()
        {
            var path = Path.GetTempFileName();

            var options = new SmugglerOptions
            {
                ShouldExcludeExpired = true
            };

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    Initialize(store);

                    var smuggler = new SmugglerApi();

					smuggler.ExportData(new SmugglerExportOptions { ToFile = path, From = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = store.DefaultDatabase } }, options).Wait(TimeSpan.FromSeconds(15));
                }

                using (var store = NewRemoteDocumentStore())
                {
                    var smuggler = new SmugglerApi();

					smuggler.ImportData(new SmugglerImportOptions { FromFile = path, To = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = store.DefaultDatabase } }, options).Wait(TimeSpan.FromSeconds(15));

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

        [Fact]
        public void SmugglerWithExcludeExpiredDocumentsShouldWork2()
        {
            var path = Path.GetTempFileName();

            var options = new SmugglerOptions
            {
                ShouldExcludeExpired = true
            };

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    Initialize(store);

                    var smuggler = new SmugglerApi();

					smuggler.ExportData(new SmugglerExportOptions { ToFile = path, From = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = store.DefaultDatabase } }, options).Wait(TimeSpan.FromSeconds(15));
                }

                using (var store = NewRemoteDocumentStore())
                {
                    SystemTime.UtcDateTime = () => DateTime.Now.AddMinutes(10);

                    var smuggler = new SmugglerApi();

					smuggler.ImportData(new SmugglerImportOptions { FromFile = path, To = new RavenConnectionStringOptions { Url = store.Url, DefaultDatabase = store.DefaultDatabase } }, options).Wait(TimeSpan.FromSeconds(15));

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