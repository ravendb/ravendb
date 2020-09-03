using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Operations.Expiration;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Tests.Core.Utils.Entities;
using Sparrow;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6292 : ReplicationTestBase
    {
        public RavenDB_6292(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task IfIncludedDocumentIsConflictedItShouldNotThrowConflictException()
        {
            using (var store1 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
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

                await WaitForConflict(store2, "addresses/1");

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
                        var command = new GetDocumentsCommand("users/1", includes: new[] { "AddressId" }, metadataOnly: false);

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

        [Fact]
        public async Task ExpirationShouldHandleConflicts()
        {
            using (var store1 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            using (var store2 = GetDocumentStore(options: new Options
            {
                ModifyDatabaseRecord = record =>
                {
                    record.ConflictSolverConfig = new ConflictSolver
                    {
                        ResolveToLatest = false,
                        ResolveByCollection = new Dictionary<string, ScriptResolver>()
                    };
                }
            }))
            {
                await SetupExpiration(store1);
                await SetupExpiration(store2);

                var expiry1 = SystemTime.UtcNow.AddMinutes(5);
                var expiry2 = SystemTime.UtcNow.AddMinutes(15);
                using (var session = store1.OpenAsyncSession())
                {
                    var company1 = new Company
                    {
                        Name = "Company Name 10"
                    };

                    await session.StoreAsync(company1, "companies/1");
                    var metadata = session.Advanced.GetMetadataFor(company1);
                    metadata[Constants.Documents.Metadata.Expires] = expiry1.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

                    var company2 = new Company
                    {
                        Name = "Company Name 11"
                    };

                    await session.StoreAsync(company2, "companies/2");
                    metadata = session.Advanced.GetMetadataFor(company2);
                    metadata[Constants.Documents.Metadata.Expires] = expiry2.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

                    await session.SaveChangesAsync();
                }

                using (var session = store2.OpenAsyncSession())
                {
                    var company = new Company
                    {
                        Name = "Company Name 20"
                    };

                    await session.StoreAsync(company, "companies/1");
                    var metadata = session.Advanced.GetMetadataFor(company);
                    metadata[Constants.Documents.Metadata.Expires] = expiry1.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

                    var company2 = new Company
                    {
                        Name = "Company Name 21"
                    };

                    await session.StoreAsync(company2, "companies/2");
                    metadata = session.Advanced.GetMetadataFor(company2);
                    metadata[Constants.Documents.Metadata.Expires] = expiry1.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite);

                    await session.SaveChangesAsync();
                }

                await SetupReplicationAsync(store1, store2);

                await WaitForConflict(store2, "companies/1");
                await WaitForConflict(store2, "companies/2");

                var database = await GetDocumentDatabaseInstanceFor(store2);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;

                await expiredDocumentsCleaner.CleanupExpiredDocs();

                using (var session = store2.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>("companies/1");
                    Assert.Null(company2);

                    await Assert.ThrowsAsync<DocumentConflictException>(() => session.LoadAsync<Company>("companies/2"));
                }
            }
        }

        private async Task SetupExpiration(DocumentStore store)
        {
            var config = new ExpirationConfiguration
            {
                Disabled = false,
                DeleteFrequencyInSec = 100,
            };

            await ExpirationHelper.SetupExpiration(store, Server.ServerStore, config);
        }
    }
}
