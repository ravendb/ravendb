// -----------------------------------------------------------------------
//  <copyright file="RDBQA_7.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.IO;

using Lucene.Net.Support;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler;
using Raven.Abstractions.Database.Smuggler.Common;
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
    public class RDBQA_7 : RavenTest
    {
        private class Product
        {
            public int Id { get; set; }

            public string Value { get; set; }
        }

        [Fact, Trait("Category", "Smuggler")]
        public void NegativeFiltersShouldNotFilterOutWhenThereAreNoMatches()
        {
            var path = Path.GetTempFileName();

            var options = new DatabaseSmugglerOptions();
            options.Filters.Add(new FilterSetting
            {
                Path = "Value",
                ShouldMatch = false,
                Values = new EquatableList<string> { "Value1" }
            });

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    Initialize(store);

                    var smuggler = new DatabaseSmuggler(
                        options, 
                        new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = store.Url,
                            Database = store.DefaultDatabase
                        }),
                        new DatabaseSmugglerFileDestination(path));

                    smuggler.Execute();
                }

                using (var store = NewRemoteDocumentStore())
                {
                    var smuggler = new DatabaseSmuggler(
                        options,
                        new DatabaseSmugglerFileSource(path),
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Url = store.Url,
                            Database = store.DefaultDatabase
                        }));

                    smuggler.Execute();

                    Assert.NotNull(store.DatabaseCommands.Get("key/1"));

                    using (var session = store.OpenSession())
                    {
                        var product1 = session.Load<Product>(1);
                        var product2 = session.Load<Product>(2);
                        var product3 = session.Load<Product>(3);

                        Assert.Null(product1);
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
        public void NegativeMetadataFiltersShouldNotFilterOutWhenThereAreNoMatches()
        {
            var path = Path.GetTempFileName();

            var options = new DatabaseSmugglerOptions();
            options.Filters.Add(new FilterSetting
            {
                Path = "@metadata.Raven-Entity-Name",
                ShouldMatch = false,
                Values = new EquatableList<string> { "Products" }
            });

            try
            {
                using (var store = NewRemoteDocumentStore())
                {
                    Initialize(store);

                    var smuggler = new DatabaseSmuggler(
                        options,
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
                        options,
                        new DatabaseSmugglerFileSource(path), 
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Database = store.DefaultDatabase,
                            Url = store.Url
                        }));

                    smuggler.Execute();

                    Assert.NotNull(store.DatabaseCommands.Get("key/1"));

                    using (var session = store.OpenSession())
                    {
                        var product1 = session.Load<Product>(1);
                        var product2 = session.Load<Product>(2);
                        var product3 = session.Load<Product>(3);

                        Assert.Null(product1);
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
            store.DatabaseCommands.Put("key/1", null, new RavenJObject(), new RavenJObject());

            var product1 = new Product { Value = "Value1" };
            var product2 = new Product { Value = "Value1" };
            var product3 = new Product { Value = "Value2" };

            using (var session = store.OpenSession())
            {
                session.Store(product1);
                session.Store(product2);
                session.Store(product3);

                session.SaveChanges();
            }
        }
    }
}