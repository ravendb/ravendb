using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Extensions;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_6292 : ReplicationTestBase
    {
        [Fact]
        public async Task IfIncludedDocumentIsConflictedItShouldNotThrowConflictException()
        {
            using (var store1 = GetDocumentStore())
            using (var store2 = GetDocumentStore())
            {
                using (var session = store1.OpenSession())
                {
                    session.Store(new Address
                    {
                        City = "New York"
                    }, "addresses/1");
                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    session.Store(new Address
                    {
                        City = "Torun"
                    }, "addresses/1");

                    session.Store(new User
                    {
                        Name = "John",
                        AddressId = "addresses/1"
                    }, "users/1");

                    session.SaveChanges();
                }

                await SetupReplicationAsync(store1, store2);

                WaitForConflict(store2, "addresses/1");

                using (var session = store2.OpenSession())
                {
                    var documentQuery = session.Advanced
                        .DocumentQuery<User>()
                        .Include(x => x.AddressId);

                    var iq = documentQuery.GetIndexQuery();

                    var user = documentQuery
                        .First();

                    Assert.Equal("John", user.Name);
                    
                    Assert.Throws<DocumentConflictException>(() => session.Load<Address>(user.AddressId));

                    using (var commands = store2.Commands())
                    {
                        var qr = commands.Query(iq);

                        var address = (BlittableJsonReaderObject)qr.Includes["addresses/1"];
                        var metadata = address.GetMetadata();
                        Assert.Equal("addresses/1", metadata.GetId());
                        Assert.True(metadata.TryGetConflict(out var conflict));
                        Assert.True(conflict);
                    }
                }

                using (var session = store2.OpenSession())
                {
                    var user = session
                        .Include<User>(x => x.AddressId)
                        .Load("users/1");

                    Assert.Equal("John", user.Name);

                    Assert.Throws<DocumentConflictException>(() => session.Load<Address>(user.AddressId));

                    using (var commands = store2.Commands())
                    {
                        var command = new GetDocumentCommand("users/1", includes: new [] {"AddressId" }, metadataOnly: false);
                        
                        commands.RequestExecutor.Execute(command, commands.Context);

                        var address = (BlittableJsonReaderObject)command.Result.Includes["addresses/1"];
                        var metadata = address.GetMetadata();
                        Assert.Equal("addresses/1", metadata.GetId());
                        Assert.True(metadata.TryGetConflict(out var conflict));
                        Assert.True(conflict);
                    }
                }
            }
        }

        private static void WaitForConflict(IDocumentStore store, string id)
        {
            var sw = Stopwatch.StartNew();
            while (sw.Elapsed < TimeSpan.FromSeconds(10))
            {
                try
                {
                    using (var session = store.OpenSession())
                    {
                        session.Load<dynamic>(id);
                    }

                    Thread.Sleep(10);
                }
                catch (ConflictException)
                {
                    return;
                }
            }

            throw new InvalidOperationException($"Waited '{sw.Elapsed}' for conflict on '{id}' but it did not happen.");
        }
    }
}
