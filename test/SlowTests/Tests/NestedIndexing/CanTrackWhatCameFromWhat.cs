// -----------------------------------------------------------------------
//  <copyright file="CanTrackWhatCameFromWhat.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.ServerWide.Context;
using Xunit;
using Voron;
using Xunit.Abstractions;
using Raven.Server.Documents.Indexes;

namespace SlowTests.Tests.NestedIndexing
{
    public class CanTrackWhatCameFromWhat : RavenTestBase
    {
        public CanTrackWhatCameFromWhat(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Id { get; set; }
            public string Ref { get; set; }
            public string Name { get; set; }
        }

        public void CreateIndex(DocumentStore store)
        {
            store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
            {
                Maps = { @"
from i in docs.Items
select new
{
    RefName = LoadDocument(i.Ref, ""Items"").Name,
    Name = i.Name
}"
                },
                Name = "test" }
            }));
        }

        [Fact]
        public async Task CrossReferencing()
        {
            using (var store = GetDocumentStore())
            {
                CreateIndex(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.Store(new Item { Id = "items/2", Ref = "items/1", Name = "ayende" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex("test");
                TransactionOperationContext context;
                using (index._contextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    var itemsCollection = "Items";
                    var item = SingleKey(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/1"), tx));
                    Assert.Equal("items/2", item.ToString());

                    item = SingleKey(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/2"), tx));
                    Assert.Equal("items/1", item.ToString());
                }
            }
        }

        [Fact]
        public async Task UpdatingDocument()
        {
            using (var store = GetDocumentStore())
            {
                CreateIndex(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex("test");
                TransactionOperationContext context;
                using (index._contextPool.AllocateOperationContext(out context))
                {
                    using (var tx = context.OpenReadTransaction())
                    {
                        var itemsCollection = "Items";
                        var item = SingleKey(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/2"), tx));
                        Assert.Equal("items/1", item);
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Load<Item>("items/1").Name = "other";
                        session.SaveChanges();
                    }

                    Indexes.WaitForIndexing(store);

                    using (var tx = context.OpenReadTransaction())
                    {
                        var itemsCollection = "Items";
                        var item = SingleKey(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/2"), tx));
                        Assert.Equal("items/1", item.ToString());
                    }
                }
            }
        }

        [Fact]
        public async Task UpdatingReferenceToAnotherDoc()
        {
            using (var store = GetDocumentStore())
            {
                CreateIndex(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
                    session.Store(new Item { Id = "items/3", Ref = null, Name = "ayende" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex("test");
                TransactionOperationContext context;
                using (index._contextPool.AllocateOperationContext(out context))
                {
                    using (var tx = context.OpenReadTransaction())
                    {
                        var itemsCollection = "Items";
                        var item = SingleKey(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/2"), tx));
                        Assert.Equal("items/1", item.ToString());
                        Assert.Empty(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/3"), tx));
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Load<Item>("items/1").Ref = "items/3";
                        session.SaveChanges();
                    }

                    Indexes.WaitForIndexing(store);

                    using (var tx = context.OpenReadTransaction())
                    {
                        var itemsCollection = "Items";
                        Assert.Empty(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/2"), tx));

                        var item = SingleKey(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/3"), tx));
                        Assert.Equal("items/1", item.ToString());
                    }
                }
            }
        }

        private string SingleKey(IEnumerable<Slice> enumerable)
        {
            using (var e = enumerable.GetEnumerator())
            {
                if (e.MoveNext() == false)
                    throw new InvalidOperationException("Expected one result, had none");
                var s = e.Current.ToString();
                if (e.MoveNext())
                    throw new InvalidOperationException("Expected one result, but got more than that");

                return s;
            }
        }

        [Fact]
        public async Task UpdatingReferenceToMissing()
        {
            using (var store = GetDocumentStore())
            {
                CreateIndex(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex("test");
                TransactionOperationContext context;
                using (index._contextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    var itemsCollection = "Items";
                    var item = SingleKey(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/2"), tx));
                    Assert.Equal("items/1", item.ToString());
                }
            }
        }

        [Fact]
        public async Task UpdatingReferenceToNull()
        {
            using (var store = GetDocumentStore())
            {
                CreateIndex(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = null, Name = "oren" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex("test");
                TransactionOperationContext context;
                using (index._contextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    var itemsCollection = "Items";
                    Assert.Empty(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/1"), tx));
                }
            }
        }

        [Fact]
        public async Task AddingReferenceToSamedoc()
        {
            using (var store = GetDocumentStore())
            {
                CreateIndex(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/1", Name = "oren" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex("test");
                TransactionOperationContext context;
                using (index._contextPool.AllocateOperationContext(out context))
                using (var tx = context.OpenReadTransaction())
                {
                    var itemsCollection = "Items";
                    Assert.Empty(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/1"), tx));
                }
            }
        }

        [Fact]
        public async Task DeletingRootDoc()
        {
            using (var store = GetDocumentStore())
            {
                CreateIndex(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "oren" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var database = await GetDatabase(store.Database);
                var index = database.IndexStore.GetIndex("test");
                TransactionOperationContext context;
                using (index._contextPool.AllocateOperationContext(out context))
                {
                    using (var tx = context.OpenReadTransaction())
                    {
                        var itemsCollection = "Items";
                        Assert.NotEmpty(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/2"), tx));
                    }

                    using (var commands = store.Commands())
                    {
                        commands.Delete("items/1", null);
                    }

                    Indexes.WaitForIndexing(store);

                    using (var tx = context.OpenReadTransaction())
                    {
                        var itemsCollection = "Items";
                        Assert.Empty(index._indexStorage.ReferencesForDocuments.GetItemKeysFromCollectionThatReference(itemsCollection, context.GetLazyString("items/2"), tx));
                    }
                }
            }
        }
    }
}
