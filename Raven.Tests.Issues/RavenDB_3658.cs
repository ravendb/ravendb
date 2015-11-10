// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3658.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;
using System.Threading.Tasks;

using Raven.Abstractions.Database.Smuggler.Common;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Smuggler.Database;
using Raven.Smuggler.Database.Remote;
using Raven.Smuggler.Database.Streams;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3658 : ReplicationBase
    {
        [Fact]
        public async Task ShouldImportHiloWhenRavenEntityNameFilterIsUsed()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new Person { Name = "John" });
                    session.Store(new Person { Name = "Edward" });
                    session.Store(new Address { Street = "Main Street" });

                    session.SaveChanges();
                }

                using (var stream = new MemoryStream())
                {
                    var options = new DatabaseSmugglerOptions();
                    options.Filters.Add(new FilterSetting
                    {
                        Path = "@metadata.Raven-Entity-Name",
                        ShouldMatch = true,
                        Values = { "People" }
                    });

                    var smuggler = new DatabaseSmuggler(
                        options, 
                        new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Database = store1.DefaultDatabase,
                            Url = store1.Url
                        }), 
                        new DatabaseSmugglerStreamDestination(stream));


                    await smuggler.ExecuteAsync();

                    stream.Position = 0;

                    smuggler = new DatabaseSmuggler(
                        options,
                        new DatabaseSmugglerStreamSource(stream),
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Database = store2.DefaultDatabase,
                            Url = store2.Url
                        }));

                    await smuggler.ExecuteAsync();
                }

                WaitForIndexing(store2);

                using (var session = store2.OpenSession())
                {
                    var people = session
                        .Query<Person>()
                        .ToList();

                    Assert.Equal(2, people.Count);

                    var addresses = session
                        .Query<Address>()
                        .ToList();

                    Assert.Equal(0, addresses.Count);

                    var hilo = session.Advanced.DocumentStore.DatabaseCommands.Get("Raven/Hilo/People");
                    Assert.NotNull(hilo);
                }
            }
        }

        [Fact]
        public async Task ShouldImportHiloWhenRavenEntityNameFilterIsUsed_Between()
        {
            using (var store1 = CreateStore())
            using (var store2 = CreateStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new Person { Name = "John" });
                    session.Store(new Person { Name = "Edward" });
                    session.Store(new Address { Street = "Main Street" });

                    session.SaveChanges();
                }

                using (var stream = new MemoryStream())
                {
                    var options = new DatabaseSmugglerOptions();
                    options.Filters.Add(new FilterSetting
                    {
                        Path = "@metadata.Raven-Entity-Name",
                        ShouldMatch = true,
                        Values = { "People" }
                    });

                    var smuggler = new DatabaseSmuggler(
                        options,
                        new DatabaseSmugglerRemoteSource(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Database = store1.DefaultDatabase,
                            Url = store1.Url
                        }),
                        new DatabaseSmugglerRemoteDestination(new DatabaseSmugglerRemoteConnectionOptions
                        {
                            Database = store2.DefaultDatabase,
                            Url = store2.Url
                        }));


                    await smuggler.ExecuteAsync();
                }

                WaitForIndexing(store2);

                using (var session = store2.OpenSession())
                {
                    var people = session
                        .Query<Person>()
                        .ToList();

                    Assert.Equal(2, people.Count);

                    var addresses = session
                        .Query<Address>()
                        .ToList();

                    Assert.Equal(0, addresses.Count);

                    var hilo = session.Advanced.DocumentStore.DatabaseCommands.Get("Raven/Hilo/People");
                    Assert.NotNull(hilo);
                }
            }
        }
    }
}
