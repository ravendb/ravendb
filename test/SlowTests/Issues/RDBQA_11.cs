// -----------------------------------------------------------------------
//  <copyright file="RDBQA_11.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using FastTests;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Smuggler;
using Raven.Json.Linq;
using Raven.Server.Documents.Expiration;
using Raven.Server.Utils;
using Xunit;

namespace SlowTests.Issues
{
    public class RDBQA_11 : RavenTestBase
    {
        private class Product
        {
            public int Id { get; set; }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void SmugglerWithoutExcludeExpiredDocumentsShouldWork()
        {
            var path = Path.GetTempFileName();

            try
            {
                using (var store = GetDocumentStore())
                {
                    Initialize(store);

                    store.Smuggler.ExportAsync(new DatabaseSmugglerOptions(), path).Wait(TimeSpan.FromSeconds(15));
                }

                using (var store = GetDocumentStore())
                {
                    store.Smuggler.ImportAsync(new DatabaseSmugglerOptions(), path).Wait(TimeSpan.FromSeconds(15));

                    using (var session = store.OpenSession())
                    {
                        var product1 = session.Load<Product>(1);
                        var product2 = session.Load<Product>(2);
                        var product3 = session.Load<Product>(3);

                        Assert.NotNull(product1);
                        Assert.NotNull(product2);
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
                using (var store = GetDocumentStore())
                {
                    Initialize(store);

                    store.Smuggler.ExportAsync(new DatabaseSmugglerOptions { IncludeExpired = false }, path).Wait(TimeSpan.FromSeconds(15));
                }

                using (var store = GetDocumentStore())
                {
                    store.Smuggler.ImportAsync(new DatabaseSmugglerOptions { IncludeExpired = false }, path).Wait(TimeSpan.FromSeconds(15));

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
                using (var store = GetDocumentStore())
                {
                    Initialize(store);

                    store.Smuggler.ExportAsync(new DatabaseSmugglerOptions { IncludeExpired = false }, path).Wait(TimeSpan.FromSeconds(15));
                }

                using (var store = GetDocumentStore())
                {
                    var database = GetDocumentDatabaseInstanceFor(store).Result;
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);

                    store.Smuggler.ImportAsync(new DatabaseSmugglerOptions { IncludeExpired = false }, path).Wait(TimeSpan.FromSeconds(15));

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
            SetupExpiration(store);

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

        private static void SetupExpiration(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new ExpirationConfiguration
                {
                    Active = true,
                    DeleteFrequencySeconds = 100,
                }, Constants.Expiration.ConfigurationDocumentKey);

                session.SaveChanges();
            }
        }
    }
}
