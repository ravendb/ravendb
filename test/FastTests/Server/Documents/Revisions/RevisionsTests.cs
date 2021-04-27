//-----------------------------------------------------------------------
// <copyright file="RevisionsTests.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Utils;
using Raven.Client;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Client.Documents.Session;
using Raven.Client.Http;
using Raven.Client.Json;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Admin;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Revisions
{
    public class RevisionsTests : RavenTestBase
    {
        public RevisionsTests(ITestOutputHelper output) : base(output)
        {
        }
        [Fact]
        public async Task CanGetRevisionsByChangeVectorsLazily()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenSession())
                {
                     session.Store(new User {Name = "Omer"}, id);
                     session.SaveChanges();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var user =  session.Load<Company>(id);
                        user.Name = "Omer" + i;
                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    var revisionsMetadata =  session.Advanced.Revisions.GetMetadataFor(id);
                    Assert.Equal(11, revisionsMetadata.Count);

                    var changeVectors = revisionsMetadata.Select(x => x.GetString(Constants.Documents.Metadata.ChangeVector)).ToList();
                    
                    var revisionsLazy  = session.Advanced.Revisions.Lazily.Get<User>(changeVectors);
                    var lazyResult =  revisionsLazy.Value;
                    var revisions  =   session.Advanced.Revisions.Get<User>(changeVectors);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(revisions.Keys, lazyResult.Keys);
                    
                    Assert.Equal(revisions.Values.Select(x=>x!=null), lazyResult.Values.Select(x=>x!=null));
                }
            }
        }
        [Fact]
        public async Task CanGetForLazily()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenSession())
                {
                     session.Store(new User {Name = "Omer"}, id);
                     session.SaveChanges();
                }
        
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var user =  session.Load<Company>(id);
                        user.Name = "Omer " + i;
                        session.SaveChanges();
                    }
                }
                using (var session = store.OpenSession())
                {
                    var revision = session.Advanced.Revisions.GetFor<User>("users/1");
                    var revisionLazily =  session.Advanced.Revisions.Lazily.GetFor<User>("users/1");
                    var revisionLazilyResult = revisionLazily.Value;

         
                    Assert.Equal(revision.Select(x => x.Name), revisionLazilyResult.Select(x => x.Name));
                    Assert.Equal(revision.Select(x => x.Id), revisionLazilyResult.Select(x => x.Id));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    
                }
            }
        }
        [Fact]
        public async Task CanGetRevisionsByIdAndTimeLazily()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenSession())
                {
                    session.Store(new User {Name = "Omer"}, id);
                    session.SaveChanges();
                }
        
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var user =  session.Load<Company>(id);
                        user.Name = "Omer " + i;
                        session.SaveChanges();
                    }
                }
                using (var session = store.OpenSession())
                {
                    var revision                 =  session.Advanced.Revisions.Get<User>("users/1", DateTime.Now);
                    var revisionLazily  =  session.Advanced.Revisions.Lazily.Get<User>("users/1",DateTime.UtcNow);
                    var revisionLazilyResult =  revisionLazily.Value;
                    
                    Assert.Equal(revision.Id, revisionLazilyResult.Id);
                    Assert.Equal(revision.Name, revisionLazilyResult.Name);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
        [Fact]
        public async Task CanGetMetadataForLazily()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenSession())
                {
                     session.Store(new User {Name = "Omer"}, id);
                     session.SaveChanges();
                }
        
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var user =  session.Load<Company>(id);
                        user.Name = "Omer " + i;
                        session.SaveChanges();
                    }
                }
                using (var session = store.OpenSession())
                {
                    var revisionsMetadata =  session.Advanced.Revisions.GetMetadataFor(id);
                    var revisionsMetaDataLazily   = session.Advanced.Revisions.Lazily.GetMetadataFor(id);
                    var revisionsMetaDataLazilyResult =  revisionsMetaDataLazily.Value;
        
                    
                    Assert.Equal(
                        revisionsMetadata.Select(x => x["@id"]),
                        revisionsMetaDataLazilyResult.Select(x =>  x["@id"])
                    );
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    
                }
            }
        }
        [Fact]
        public async Task CanGetRevisionsByChangeVectorLazily()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenSession())
                {
                     session.Store(new User {Name = "Omer"}, id);
                     session.SaveChanges();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        var user =  session.Load<Company>(id);
                        user.Name = "Omer " + i;
                        session.SaveChanges();
                    }
                }

                var database =  await GetDocumentDatabaseInstanceFor(store);
                var dbId =  database.DbBase64Id;
                var cv = $"A:21-{dbId}";

                using (var session = store.OpenSession())
                {
                    var revisions =  session.Advanced.Revisions.Get<User>(cv);
                    var revisionsLazily = session.Advanced.Revisions.Lazily.Get<User>(cv);
                    var revisionsLazilyValue =  revisionsLazily.Value;

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(revisions.Id, revisionsLazilyValue.Id);
                    Assert.Equal(revisions.Name, revisionsLazilyValue.Name);
                }
            }
        }
        [Fact]
        public async Task CanGetForAsyncLazily()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Omer"}, id);
                    await session.SaveChangesAsync();
                }
        
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "Omer " + i;
                        await session.SaveChangesAsync();
                    }
                }
                using (var session = store.OpenAsyncSession())
                {
                    var revision = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    var revisionLazily = session.Advanced.Revisions.Lazily.GetForAsync<User>("users/1");
                    var revisionLazilyResult = await revisionLazily.Value;

         
                    Assert.Equal(revision.Select(x => x.Name), revisionLazilyResult.Select(x => x.Name));
                    Assert.Equal(revision.Select(x => x.Id), revisionLazilyResult.Select(x => x.Id));
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    
                }
            }
        }
        [Fact]
        public async Task CanGetMetadataForAsyncLazily()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Omer"}, id);
                    await session.SaveChangesAsync();
                }
        
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "Omer " + i;
                        await session.SaveChangesAsync();
                    }
                }
                using (var session = store.OpenAsyncSession())
                {
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    var revisionsMetaDataLazily   = session.Advanced.Revisions.Lazily.GetMetadataForAsync(id);
                    var revisionsMetaDataLazilyResult = await revisionsMetaDataLazily.Value;
        
                    
                    Assert.Equal(
                        revisionsMetadata.Select(x => x["@id"]),
                        revisionsMetaDataLazilyResult.Select(x =>  x["@id"])
                        );
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    
                }
            }
        }
        [Fact]
        public async Task CanGetRevisionsByIdAndTimeAsyncLazily()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Omer"}, id);
                    await session.SaveChangesAsync();
                }
        
                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "Omer " + i;
                        await session.SaveChangesAsync();
                    }
                }
                using (var session = store.OpenAsyncSession())
                {
                    var revision = await session.Advanced.Revisions.GetAsync<User>("users/1", DateTime.Now);
                    var revisionLazily = session.Advanced.Revisions.Lazily.GetAsync<User>("users/1",DateTime.UtcNow);
                    var revisionLazilyResult = await revisionLazily.Value;
                    
                    Assert.Equal(revision.Id, revisionLazilyResult.Id);
                    Assert.Equal(revision.Name, revisionLazilyResult.Name);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                }
            }
        }
        [Fact]
        public async Task CanGetRevisionsByChangeVectorAsyncLazily()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Omer"}, id);
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "Omer " + i;
                        await session.SaveChangesAsync();
                    }
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                var dbId = database.DbBase64Id;
                var cv = $"A:21-{dbId}";

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetAsync<User>(cv);
                    var revisionsLazily = session.Advanced.Revisions.Lazily.GetAsync<User>(cv);
                    var revisionsLazilyValue = await revisionsLazily.Value;

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(revisions.Id, revisionsLazilyValue.Id);
                    Assert.Equal(revisions.Name, revisionsLazilyValue.Name);
                }
            }
        }

        [Fact]
        public async Task CanGetRevisionsByChangeVectorsAsyncLazily()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Omer"}, id);
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "Omer" + i;
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(11, revisionsMetadata.Count);

                    var changeVectors = revisionsMetadata.Select(x => x.GetString(Constants.Documents.Metadata.ChangeVector)).ToList();
                    
                    var revisionsLazy  = session.Advanced.Revisions.Lazily.GetAsync<User>(changeVectors);
                    var lazyResult = await revisionsLazy.Value;
                    var revisions  =  await session.Advanced.Revisions.GetAsync<User>(changeVectors);

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.Equal(revisions.Keys, lazyResult.Keys);
                    
                    Assert.Equal(revisions.Values.Select(x=>x!=null), lazyResult.Values.Select(x=>x!=null));
                }
            }
        }

        [Fact]
        public async Task CanGetRevisionsByChangeVectors()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Fitzchak"}, id);
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "Fitzchak " + i;
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(11, revisionsMetadata.Count);
                    
                    var changeVectors = revisionsMetadata.Select(x => x.GetString(Constants.Documents.Metadata.ChangeVector)).ToList();
                    changeVectors.Add("NotExistsChangeVector");
                    
                    var revisions = await session.Advanced.Revisions.GetAsync<User>(changeVectors);
                    var first = revisions.First();
                    var last = revisions.Last();
                    Assert.NotNull(first.Value);
                    Assert.Null(last.Value);

                    Assert.NotNull(await session.Advanced.Revisions.GetAsync<User>(first.Key));
                    Assert.Null(await session.Advanced.Revisions.GetAsync<User>(last.Key));
                }
            }
        }

        [Fact]
        public async Task ZeroMinimumRevisionsToKeepShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration {Disabled = false, MinimumRevisionsToKeep = 10},
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Users"] = new RevisionsCollectionConfiguration {Disabled = false, MinimumRevisionsToKeep = 0}
                    }
                };

                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "foo");
                    await session.StoreAsync(new Product(), "bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("foo");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    Assert.False(metadata.Keys.Contains(Constants.Documents.Metadata.Flags));
                    var foo = await session.Advanced.Revisions.GetMetadataForAsync("foo");
                    Assert.Equal(0, foo.Count);

                    var product = await session.LoadAsync<Product>("bar");
                    metadata = session.Advanced.GetMetadataFor(product);
                    Assert.Equal((DocumentFlags.HasRevisions).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var bar = await session.Advanced.Revisions.GetMetadataForAsync("bar");
                    Assert.Equal(1, bar.Count);
                }
            }
        }

        [Fact]
        public async Task EnforceRevisionConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration {Default = new RevisionsCollectionConfiguration {Disabled = false, MinimumRevisionsToKeep = 10}};

                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                for (int i = 0; i < 10; i++)
                {
                    for (int j = i; j >= 0; j--)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(new Product {Description = j.ToString()}, "bar" + i);
                            await session.SaveChangesAsync();
                        }
                    }
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var product = await session.LoadAsync<Product>("bar" + i);
                        var metadata = session.Advanced.GetMetadataFor(product);
                        Assert.Equal((DocumentFlags.HasRevisions).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                        var bar = await session.Advanced.Revisions.GetMetadataForAsync("bar" + i);
                        Assert.Equal(i + 1, bar.Count);
                    }
                }

                configuration.Default.MinimumRevisionsToKeep = 5;
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                    await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession(new SessionOptions
                    {
                        NoCaching = true // we aren't changing the document only it's revisions, so we need to disable caching otherwise we will get 'Not-Modified'
                    }))
                    {
                        var product = await session.LoadAsync<Product>("bar" + i);
                        var metadata = session.Advanced.GetMetadataFor(product);
                        Assert.True(metadata.Keys.Contains(Constants.Documents.Metadata.Flags));
                        var foo = await session.Advanced.Revisions.GetMetadataForAsync("bar" + i);
                        Assert.Equal(Math.Min(i + 1, 5), foo.Count);
                    }
                }

                configuration.Default.MinimumRevisionsToKeep = 0;
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                    await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var product = await session.LoadAsync<Product>("bar" + i);
                        var metadata = session.Advanced.GetMetadataFor(product);
                        Assert.False(metadata.Keys.Contains(Constants.Documents.Metadata.Flags));
                        var foo = await session.Advanced.Revisions.GetMetadataForAsync("bar" + i);
                        Assert.Equal(0, foo.Count);
                    }
                }
            }
        }

        [Fact]
        public async Task EnforceRevisionConfigurationWithTombstones()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration {Default = new RevisionsCollectionConfiguration {Disabled = false, MinimumRevisionsToKeep = 10}};

                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                for (int i = 0; i < 10; i++)
                {
                    for (int j = i; j >= 0; j--)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(new Product {Description = j.ToString()}, "bar" + i);
                            await session.SaveChangesAsync();
                        }
                    }
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var product = await session.LoadAsync<Product>("bar" + i);
                        var metadata = session.Advanced.GetMetadataFor(product);
                        Assert.Equal((DocumentFlags.HasRevisions).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                        var bar = await session.Advanced.Revisions.GetMetadataForAsync("bar" + i);
                        Assert.Equal(i + 1, bar.Count);
                    }
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        session.Delete("bar" + i);
                        await session.SaveChangesAsync();
                    }
                }

                configuration.Default.MinimumRevisionsToKeep = 5;
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                    await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var foo = await session.Advanced.Revisions.GetMetadataForAsync("bar" + i);
                        Assert.Equal(Math.Min(i + 2, 5), foo.Count);
                    }
                }

                configuration.Default.MinimumRevisionsToKeep = 0;
                configuration.Default.PurgeOnDelete = true;
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                    await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var foo = await session.Advanced.Revisions.GetMetadataForAsync("bar" + i);
                        Assert.Equal(0, foo.Count);
                    }
                }
            }
        }

        [Fact]
        public async Task EnforceEmptyConfigurationWillDeleteAllRevisions()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Default = new RevisionsCollectionConfiguration {Disabled = false, MinimumRevisionsToKeep = 10},
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Dummy"] = new RevisionsCollectionConfiguration {Disabled = false, MinimumRevisionsToKeep = 10}
                    }
                };

                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Product(), "foo/bar");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var product = await session.LoadAsync<Product>("foo/bar");
                    var metadata = session.Advanced.GetMetadataFor(product);
                    Assert.Equal((DocumentFlags.HasRevisions).ToString(), metadata[Constants.Documents.Metadata.Flags]);
                    var bar = await session.Advanced.Revisions.GetMetadataForAsync("foo/bar");
                    Assert.Equal(1, bar.Count);
                }

                configuration.Default = null;
                await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration);

                var db = await GetDocumentDatabaseInstanceFor(store);
                using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
                    await db.DocumentsStorage.RevisionsStorage.EnforceConfiguration(_ => { }, token);

                using (var session = store.OpenAsyncSession())
                {
                    var product = await session.LoadAsync<Product>("foo/bar");
                    var metadata = session.Advanced.GetMetadataFor(product);
                    Assert.False(metadata.Keys.Contains(Constants.Documents.Metadata.Flags));
                    var foo = await session.Advanced.Revisions.GetMetadataForAsync("foo/bar");
                    Assert.Equal(0, foo.Count);
                }
            }
        }

        [Fact]
        public async Task CanGetNullForNotExistsDocument()
        {
            using (var store = GetDocumentStore())
            {
                var id = "users/1";
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<User>(id);
                    Assert.NotNull(revisions);
                    Assert.Empty(revisions);

                    var metadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.NotNull(metadata);
                    Assert.Empty(metadata);
                }
            }
        }

        [Fact]
        public async Task CanGetAllRevisionsFor()
        {
            var company = new Company {Name = "Company Name"};
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    company3.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(2, companiesRevisions.Count);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[0].Name);
                    Assert.Equal("Company Name", companiesRevisions[1].Name);
                }
            }
        }

        [Fact]
        public async Task CanCheckIfDocumentHasRevisions()
        {
            var company = new Company {Name = "Company Name"};
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    var metadata = session.Advanced.GetMetadataFor(company3);

                    Assert.Equal(DocumentFlags.HasRevisions.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                }
            }
        }

        [Fact]
        public async Task RemoveHasRevisionsFlag()
        {
            var company = new Company {Name = "Company Name"};
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    var metadata = session.Advanced.GetMetadataFor(company3);

                    Assert.Equal(DocumentFlags.HasRevisions.ToString(), metadata.GetString(Constants.Documents.Metadata.Flags));
                }

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, (config) => { config.Default.MinimumRevisionsToKeep = 0; });
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    var metadata = session.Advanced.GetMetadataFor(company3);

                    Assert.False(metadata.TryGetValue(Constants.Documents.Metadata.Flags, out _));
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesList = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(0, companiesList.Count);
                }
            }
        }

        [Fact]
        public async Task GetRevisionsOfNotExistKey()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("companies/1");
                    Assert.Equal(0, companiesRevisions.Count);
                }
            }
        }

        [Fact]
        public async Task CanExcludeEntitiesFromRevisions()
        {
            var user = new User {Name = "User Name"};
            var comment = new Comment {Name = "foo"};
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.StoreAsync(comment);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    Assert.Empty(await session.Advanced.Revisions.GetForAsync<Comment>(comment.Id));
                    var users = await session.Advanced.Revisions.GetForAsync<User>(user.Id);
                    Assert.Equal(1, users.Count);
                }
            }
        }

        [Fact]
        public async Task ServerSaveBundlesAfterRestart()
        {
            var path = NewDataPath();
            var company = new Company {Name = "Company Name"};
            using (var store = GetDocumentStore(new Options {Path = path}))
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    company3.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }

                var old = GetDocumentDatabaseInstanceFor(store).Result;
                Server.ServerStore.DatabasesLandlord.UnloadDirectly(store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(2, companiesRevisions.Count);
                    Assert.Equal("Hibernating Rhinos", companiesRevisions[0].Name);
                    Assert.Equal("Company Name", companiesRevisions[1].Name);
                }

                var newInstance = GetDocumentDatabaseInstanceFor(store).Result;

                Assert.NotSame(old, newInstance);
            }
        }

        [Fact]
        public async Task WillCreateRevision()
        {
            var product = new User {Name = "Hibernating"};
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    product.Name += " Rhinos";
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    product.Name += " - RavenDB";
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Advanced.Revisions.GetForAsync<User>(product.Id);
                    Assert.Equal(3, users.Count);
                    Assert.Equal("Hibernating Rhinos - RavenDB", users[0].Name);
                    Assert.Equal("Hibernating Rhinos", users[1].Name);
                    Assert.Equal("Hibernating", users[2].Name);
                }
            }
        }

        [Fact]
        public async Task WillCreateValidRevisionWhenCompressionDocumentWasSaved()
        {
            var user = new User {Name = new string('1', 4096 * 2)}; // create a string which will be compressed
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(user);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var actualUser = await session.LoadAsync<User>(user.Id);
                    Assert.Equal(actualUser.Name, user.Name);

                    var users = await session.Advanced.Revisions.GetForAsync<User>(user.Id);
                    Assert.Equal(user.Name, users.Single().Name);
                }
            }
        }

        [Fact]
        public async Task WillNotCreateRevision()
        {
            var product = new Product {Description = "A fine document db", Quantity = 5};
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    product.Description = "desc 2";
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    product.Description = "desc 3";
                    await session.StoreAsync(product);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var products = await session.Advanced.Revisions.GetForAsync<Product>(product.Id);
                    Assert.Equal(0, products.Count);
                }
            }
        }

        [Fact]
        public async Task WillDeleteOldRevisions()
        {
            var company = new Company {Name = "Company #1"};
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                    for (var i = 0; i < 10; i++)
                    {
                        company.Name = "Company #2: " + i;
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisions = await session.Advanced.Revisions.GetForAsync<Company>(company.Id);
                    Assert.Equal(5, revisions.Count);
                    Assert.Equal("Company #2: 9", revisions[0].Name);
                    Assert.Equal("Company #2: 5", revisions[4].Name);
                }
            }
        }

        [Fact]
        public async Task WillDeleteRevisionsIfDeleted_OnlyIfPurgeOnDeleteIsTrue()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                using (var session = store.OpenAsyncSession())
                {
                    var company = new Company {Name = "Hibernating Rhinos "};
                    var user = new User {Name = "Fitzchak "};
                    await session.StoreAsync(company, "companies/1");
                    await session.StoreAsync(user, "users/1");
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var company = await session.LoadAsync<Company>("companies/1");
                        var user = await session.LoadAsync<User>("users/1");
                        company.Name += i;
                        user.Name += i;
                        await session.StoreAsync(company);
                        await session.StoreAsync(user);
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1");
                    var user = await session.LoadAsync<User>("users/1");
                    Assert.NotNull(company);
                    Assert.NotNull(user);
                    session.Delete(company);
                    session.Delete(user);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companies = await session.Advanced.Revisions.GetForAsync<Company>("companies/1");
                    var users = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(5, companies.Count);
                    Assert.Empty(users);
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company {Name = "New Company"}, "companies/1");
                    await session.StoreAsync(new User {Name = "New User"}, "users/1");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companies = await session.Advanced.Revisions.GetForAsync<Company>("companies/1");
                    var users = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(5, companies.Count);
                    Assert.Equal("New Company", companies.First().Name);
                    Assert.Equal(1, users.Count);
                }
            }
        }

        [Fact]
        public async Task RevisionsOrder()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Hibernating"}, "users/1");
                    await session.StoreAsync(new User {Name = "Hibernating11"}, "users/11");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Hibernating Rhinos"}, "users/1");
                    await session.StoreAsync(new User {Name = "Hibernating Rhinos11"}, "users/11");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "Hibernating Rhinos - RavenDB"}, "users/1");
                    await session.StoreAsync(new User {Name = "Hibernating Rhinos - RavenDB11"}, "users/11");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Advanced.Revisions.GetForAsync<User>("users/1");
                    Assert.Equal(3, users.Count);
                    Assert.Equal("Hibernating Rhinos - RavenDB", users[0].Name);
                    Assert.Equal("Hibernating Rhinos", users[1].Name);
                    Assert.Equal("Hibernating", users[2].Name);
                }
            }
        }

        [Fact]
        public async Task CanGetCountersSnapshotInRevisions()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    // revision 1
                    await session.StoreAsync(new Company(), "companies/1-A");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1-A");

                    // revision 2
                    company.Name = "HR";
                    await session.SaveChangesAsync();

                    // revision 3
                    session.CountersFor(company).Increment("Likes", 100);
                    await session.SaveChangesAsync();

                    // no revision for this one
                    session.CountersFor(company).Increment("Likes", 50);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("companies/1-A");
                    Assert.Equal(3, companiesRevisions.Count);
                    var metadatas = companiesRevisions.Select(c => session.Advanced.GetMetadataFor(c)).ToList();

                    Assert.Equal("HR", companiesRevisions[0].Name);

                    var revisionCounters = (IMetadataDictionary)metadatas[0][Constants.Documents.Metadata.RevisionCounters];
                    Assert.Equal(1, revisionCounters.Count);
                    Assert.Equal(100L, revisionCounters["Likes"]);

                    Assert.Equal("HR", companiesRevisions[1].Name);
                    Assert.False(metadatas[1].TryGetValue(Constants.Documents.Metadata.RevisionCounters, out _));

                    Assert.Null(companiesRevisions[2].Name);
                    Assert.False(metadatas[1].TryGetValue(Constants.Documents.Metadata.RevisionCounters, out _));
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company = await session.LoadAsync<Company>("companies/1-A");

                    // revision 4
                    company.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();

                    // revision 5
                    session.CountersFor(company).Increment("Dislikes", 20);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisions = await session.Advanced.Revisions.GetForAsync<Company>("companies/1-A");
                    Assert.Equal(5, companiesRevisions.Count);
                    var metadatas = companiesRevisions.Select(c => session.Advanced.GetMetadataFor(c)).ToList();

                    Assert.Equal("Hibernating Rhinos", companiesRevisions[0].Name);
                    var revisionCounters = (IMetadataDictionary)metadatas[0][Constants.Documents.Metadata.RevisionCounters];
                    Assert.Equal(2, revisionCounters.Count);
                    Assert.Equal(150L, revisionCounters["Likes"]);
                    Assert.Equal(20L, revisionCounters["Dislikes"]);

                    Assert.Equal("Hibernating Rhinos", companiesRevisions[1].Name);
                    revisionCounters = (IMetadataDictionary)metadatas[1][Constants.Documents.Metadata.RevisionCounters];
                    Assert.Equal(1, revisionCounters.Count);
                    Assert.Equal(150L, revisionCounters["Likes"]);

                    Assert.Equal("HR", companiesRevisions[2].Name);
                    revisionCounters = (IMetadataDictionary)metadatas[2][Constants.Documents.Metadata.RevisionCounters];
                    Assert.Equal(1, revisionCounters.Count);
                    Assert.Equal(100L, revisionCounters["Likes"]);

                    Assert.Equal("HR", companiesRevisions[3].Name);
                    Assert.False(metadatas[3].TryGetValue(Constants.Documents.Metadata.RevisionCounters, out _));

                    Assert.Null(companiesRevisions[4].Name);
                    Assert.False(metadatas[4].TryGetValue(Constants.Documents.Metadata.RevisionCounters, out _));
                }
            }
        }

        [Fact]
        public async Task CanLimitNumberOfRevisionsByAge()
        {
            var revisionsAgeLimit = TimeSpan.FromSeconds(10);

            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["Users"] = new RevisionsCollectionConfiguration {Disabled = false, MinimumRevisionAgeToKeep = revisionsAgeLimit}
                    }
                };
                var index = await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration);

                var documentDatabase = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
                await documentDatabase.RachisLogIndexNotifications.WaitForIndexNotification(index, Server.ServerStore.Engine.OperationTimeout);

                using (var session = store.OpenAsyncSession())
                {
                    // revision 1
                    await session.StoreAsync(new User {Name = "Aviv"}, "users/1-A");
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1-A");

                    // revision 2
                    user.Name = "Aviv2";

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var userRevisions = await session.Advanced.Revisions.GetForAsync<User>("users/1-A");
                    Assert.Equal(2, userRevisions.Count);
                    Assert.Equal("Aviv2", userRevisions[0].Name);
                    Assert.Equal("Aviv", userRevisions[1].Name);
                }

                var database = await GetDatabase(store.Database);
                database.Time.UtcDateTime = () => DateTime.UtcNow.Add(revisionsAgeLimit);

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<User>("users/1-A");

                    // revision 3
                    user.Name = "Aviv3";

                    // revisions age limit has passed
                    // should delete the old revisions now
                    // and keep just this one

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var userRevisions = await session.Advanced.Revisions.GetForAsync<User>("users/1-A");
                    Assert.Equal(1, userRevisions.Count);
                    Assert.Equal("Aviv3", userRevisions[0].Name);
                }
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task GetRevisionsBinEntries(bool useSession)
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database,
                    modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);

                var deletedRevisions = await store.Commands().GetRevisionsBinEntriesAsync(long.MaxValue);
                Assert.Equal(0, deletedRevisions.Count());

                var id = "users/1";
                if (useSession)
                {
                    var user = new User {Name = "Fitzchak"};
                    for (var i = 0; i < 2; i++)
                    {
                        using (var session = store.OpenAsyncSession())
                        {
                            await session.StoreAsync(user);
                            await session.SaveChangesAsync();
                        }

                        using (var session = store.OpenAsyncSession())
                        {
                            session.Delete(user.Id);
                            await session.SaveChangesAsync();
                        }
                    }

                    id += "-A";
                }
                else
                {
                    await store.Commands().PutAsync(id, null, new User {Name = "Fitzchak"});
                    await store.Commands().DeleteAsync(id, null);
                    await store.Commands().PutAsync(id, null, new User {Name = "Fitzchak"});
                    await store.Commands().DeleteAsync(id, null);
                }

                var statistics = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(useSession ? 1 : 0, statistics.CountOfDocuments);
                Assert.Equal(4, statistics.CountOfRevisionDocuments);

                deletedRevisions = await store.Commands().GetRevisionsBinEntriesAsync(long.MaxValue);
                Assert.Equal(1, deletedRevisions.Count());

                using (var session = store.OpenAsyncSession())
                {
                    var users = await session.Advanced.Revisions.GetForAsync<User>(id);
                    Assert.Equal(4, users.Count);
                    Assert.Equal(null, users[0].Name);
                    Assert.Equal("Fitzchak", users[1].Name);
                    Assert.Equal(null, users[2].Name);
                    Assert.Equal("Fitzchak", users[3].Name);

                    // Can get metadata only
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(4, revisionsMetadata.Count);
                    Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata[0].GetString(Constants.Documents.Metadata.Flags));
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision).ToString(), revisionsMetadata[1].GetString(Constants.Documents.Metadata.Flags));
                    Assert.Contains(DocumentFlags.DeleteRevision.ToString(), revisionsMetadata[2].GetString(Constants.Documents.Metadata.Flags));
                    Assert.Equal((DocumentFlags.HasRevisions | DocumentFlags.Revision).ToString(), revisionsMetadata[3].GetString(Constants.Documents.Metadata.Flags));
                }

                await store.Maintenance.SendAsync(new DeleteRevisionsOperation(new AdminRevisionsHandler.Parameters {DocumentIds = new[] {id, "users/not/exists"}}));

                statistics = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(useSession ? 1 : 0, statistics.CountOfDocuments);
                Assert.Equal(0, statistics.CountOfRevisionDocuments);
            }
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public async Task DeleteRevisionsBeforeFromConsole(bool useConsole)
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database,
                    modifyConfiguration: configuration => configuration.Collections["Users"].PurgeOnDelete = false);

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(-1);

                for (var i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User {Name = "Fitzchak " + i});
                        await session.SaveChangesAsync();
                    }
                }

                database.Time.UtcDateTime = () => DateTime.UtcNow.AddDays(1);
                for (var i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        await session.StoreAsync(new User {Name = "Fitzchak " + (i + 100)});
                        await session.SaveChangesAsync();
                    }
                }

                database.Time.UtcDateTime = () => DateTime.UtcNow;

                var statistics = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(21, statistics.CountOfDocuments);
                Assert.Equal(20, statistics.CountOfRevisionDocuments);

                if (useConsole)
                {
                    new AdminJsConsole(Server, database).ApplyScript(new AdminJsScript(
                        "database.DocumentsStorage.RevisionsStorage.Operations.DeleteRevisionsBefore('Users', new Date());"));
                }
                else
                {
                    database.DocumentsStorage.RevisionsStorage.Operations.DeleteRevisionsBefore("Users", DateTime.UtcNow);
                }

                statistics = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(21, statistics.CountOfDocuments);
                Assert.Equal(10, statistics.CountOfRevisionDocuments);
            }
        }

        [Fact]
        public async Task DeleteRevisionsWhenNoneExistShouldNotThrow()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User(), "foo/bar");
                    await session.SaveChangesAsync();
                }

                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);

                var database = await GetDocumentDatabaseInstanceFor(store);
                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    database.DocumentsStorage.RevisionsStorage.DeleteRevisionsFor(ctx, "foo/bar");
                }
            }
        }

        [Fact]
        public async Task CollectionCaseSensitiveTest1()
        {
            using (var store = GetDocumentStore())
            {
                var id = "user/1";
                var configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration> {["uSErs"] = new RevisionsCollectionConfiguration {Disabled = false}}
                };
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration);


                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "raven"}, id);
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "raven " + i;
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(11, revisionsMetadata.Count);
                }
            }
        }

        [Fact]
        public async Task CollectionCaseSensitiveTest2()
        {
            using (var store = GetDocumentStore())
            {
                var id = "uSEr/1";
                var configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration> {["users"] = new RevisionsCollectionConfiguration {Disabled = false}}
                };
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration);


                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User {Name = "raven"}, id);
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<Company>(id);
                        user.Name = "raven " + i;
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var revisionsMetadata = await session.Advanced.Revisions.GetMetadataForAsync(id);
                    Assert.Equal(11, revisionsMetadata.Count);
                }
            }
        }

        [Fact]
        public void CollectionCaseSensitiveTest3()
        {
            using (var store = GetDocumentStore())
            {
                var configuration = new RevisionsConfiguration
                {
                    Collections = new Dictionary<string, RevisionsCollectionConfiguration>
                    {
                        ["users"] = new RevisionsCollectionConfiguration {Disabled = false}, ["USERS"] = new RevisionsCollectionConfiguration {Disabled = false}
                    }
                };

                var argumentException = Assert.ThrowsAsync<ArgumentException>(() =>
                    RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, configuration));
            }
        }

        [Fact]
        public async Task CanGetAllRevisionsForDocument_UsingStoreOperation()
        {
            var company = new Company {Name = "Company Name"};
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(company.Id);
                    company3.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }

                var revisionsResult = await store.Operations.SendAsync(new GetRevisionsOperation<Company>(company.Id));

                Assert.Equal(2, revisionsResult.TotalResults);

                var companiesRevisions = revisionsResult.Results;
                Assert.Equal(2, companiesRevisions.Count);
                Assert.Equal("Hibernating Rhinos", companiesRevisions[0].Name);
                Assert.Equal("Company Name", companiesRevisions[1].Name);
            }
        }

        [Fact]
        public async Task CanGetRevisionsWithPaging_UsingStoreOperation()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, minRevisionToKeep: 100);
                var id = "companies/1";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), id);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>(id);
                    company2.Name = "Hibernating";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company3 = await session.LoadAsync<Company>(id);
                    company3.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 10; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var company = await session.LoadAsync<Company>(id);
                        company.Name = "HR" + i;
                        await session.SaveChangesAsync();
                    }
                }

                var parameters = new GetRevisionsOperation<Company>.Parameters {Id = id, Start = 10};
                var revisionsResult = await store.Operations.SendAsync(new GetRevisionsOperation<Company>(parameters));
                Assert.Equal(13, revisionsResult.TotalResults);

                var companiesRevisions = revisionsResult.Results;
                Assert.Equal(3, companiesRevisions.Count);

                Assert.Equal("Hibernating Rhinos", companiesRevisions[0].Name);
                Assert.Equal("Hibernating", companiesRevisions[1].Name);
                Assert.Null(companiesRevisions[2].Name);
            }
        }

        [Fact]
        public async Task CanGetRevisionsWithPaging2_UsingStoreOperation()
        {
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database, minRevisionToKeep: 100);
                var id = "companies/1";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Company(), id);
                    await session.SaveChangesAsync();
                }

                for (int i = 0; i < 99; i++)
                {
                    using (var session = store.OpenAsyncSession())
                    {
                        var company = await session.LoadAsync<Company>(id);
                        company.Name = "HR" + i;
                        await session.SaveChangesAsync();
                    }
                }

                var revisionsResult = await store.Operations.SendAsync(new GetRevisionsOperation<Company>(id, start: 50, pageSize: 10));
                Assert.Equal(100, revisionsResult.TotalResults);

                var companiesRevisions = revisionsResult.Results;
                Assert.Equal(10, companiesRevisions.Count);

                var count = 0;
                for (int i = 48; i > 38; i--)
                {
                    Assert.Equal("HR" + i, companiesRevisions[count++].Name);
                }
            }
        }

        [Fact]
        public void CanGetRevisionsCountFor()
        {
            var company = new Company {Name = "Company Name"};
            using (var store = GetDocumentStore())
            {
                RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database).Wait();

                using (var session = store.OpenSession())
                {
                    session.Store(company);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company2 = session.Load<Company>(company.Id);
                    company2.Address1 = "Israel";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var company3 = session.Load<Company>(company.Id);
                    company3.Name = "Hibernating Rhinos";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var companiesRevisionsCount = session.Advanced.Revisions.GetCountFor(company.Id);
                    Assert.Equal(3, companiesRevisionsCount);
                }
            }
        }

        [Fact]
        public async Task CanGetRevisionsCountForAsync()
        {
            var company = new Company {Name = "Company Name"};
            using (var store = GetDocumentStore())
            {
                await RevisionsHelper.SetupRevisions(Server.ServerStore, store.Database);
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(company);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var company2 = await session.LoadAsync<Company>(company.Id);
                    company2.Name = "Hibernating Rhinos";
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var companiesRevisionsCount = await session.Advanced.Revisions.GetCountForAsync(company.Id);
                    Assert.Equal(2, companiesRevisionsCount);
                }
            }
        }

        public class DeleteRevisionsOperation : IMaintenanceOperation
        {
            private readonly AdminRevisionsHandler.Parameters _parameters;

            public DeleteRevisionsOperation(AdminRevisionsHandler.Parameters parameters)
            {
                _parameters = parameters;
            }

            public RavenCommand GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new DeleteRevisionsCommand(conventions, context, _parameters);
            }

            private class DeleteRevisionsCommand : RavenCommand
            {
                private readonly BlittableJsonReaderObject _parameters;

                public DeleteRevisionsCommand(DocumentConventions conventions, JsonOperationContext context, AdminRevisionsHandler.Parameters parameters)
                {
                    if (conventions == null)
                        throw new ArgumentNullException(nameof(conventions));
                    if (context == null)
                        throw new ArgumentNullException(nameof(context));
                    if (parameters == null)
                        throw new ArgumentNullException(nameof(parameters));

                    _parameters = DocumentConventions.Default.Serialization.DefaultConverter.ToBlittable(parameters, context);
                }

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/admin/revisions";

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Delete,
                        Content = new BlittableJsonContent(async stream => await ctx.WriteAsync(stream, _parameters).ConfigureAwait(false))
                    };
                }
            }
        }

        private class Comment
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class Product
        {
            public string Id { get; set; }
            public string Description { get; set; }
            public int Quantity { get; set; }
        }
    }
}
