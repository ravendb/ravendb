// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4227.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_4227 : RavenTest
    {
        [Theory]
        [PropertyData("Storages")]
        public void cache_leftovers_after_document_change(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(true, requestedStorage: requestedStorage))
            {
                store.DatabaseCommands.Admin.StopIndexing();
                Assert.Equal(0, GetCachedItemsCount(store));

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { CompanyId = "companies/1" });
                    session.SaveChanges();
                    // not in cache until we load the document
                    Assert.Equal(0, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    Assert.Equal(1, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    order.CompanyId = "companies/2";
                    session.SaveChanges();
                    Assert.Equal(0, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    Assert.Equal(1, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    order.CompanyId = "companies/3";
                    session.SaveChanges();
                    Assert.Equal(0, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    Assert.Equal(1, GetCachedItemsCount(store));
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void cache_leftovers_after_documents_change(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(true, requestedStorage: requestedStorage))
            {
                store.DatabaseCommands.Admin.StopIndexing();
                Assert.Equal(0, GetCachedItemsCount(store));

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { CompanyId = "companies/1" });
                    session.Store(new Order { CompanyId = "companies/1" });
                    session.SaveChanges();
                    // not in cache until we load the documents
                    Assert.Equal(0, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    Assert.Equal(1, GetCachedItemsCount(store));

                    session.Load<Order>("orders/2");
                    Assert.Equal(2, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    order.CompanyId = "companies/2";
                    session.SaveChanges();
                    Assert.Equal(1, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    Assert.Equal(2, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/2");
                    order.CompanyId = "companies/2";
                    session.SaveChanges();
                    Assert.Equal(1, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/2");
                    Assert.Equal(2, GetCachedItemsCount(store));
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void cache_leftovers_after_document_bulk_insert(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(true, requestedStorage: requestedStorage))
            {
                store.DatabaseCommands.Admin.StopIndexing();
                Assert.Equal(0, GetCachedItemsCount(store));

                var bulkOptions = new BulkInsertOptions
                {
                    OverwriteExisting = true
                };
                using (var bulk = store.BulkInsert(options: bulkOptions))
                {
                    bulk.Store(new Order { CompanyId = "companies/1" });
                }
                // not in cache until we load the document
                Assert.Equal(0, GetCachedItemsCount(store));

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    Assert.Equal(1, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    order.CompanyId = "companies/2";
                    using (var bulk = store.BulkInsert(options: bulkOptions))
                        bulk.Store(order);

                    Assert.Equal(0, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    Assert.Equal(1, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    order.CompanyId = "companies/3";
                    using (var bulk = store.BulkInsert(options: bulkOptions))
                        bulk.Store(order);

                    Assert.Equal(0, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    Assert.Equal(1, GetCachedItemsCount(store));
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void cache_leftovers_after_documents_bulk_insert(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(true, requestedStorage: requestedStorage))
            {
                store.DatabaseCommands.Admin.StopIndexing();
                Assert.Equal(0, GetCachedItemsCount(store));

                var bulkOptions = new BulkInsertOptions
                {
                    OverwriteExisting = true
                };
                using (var bulk = store.BulkInsert(options: bulkOptions))
                {
                    bulk.Store(new Order { CompanyId = "companies/1" });
                    bulk.Store(new Order { CompanyId = "companies/1" });
                }
                // not in cache until we load the documents
                Assert.Equal(0, GetCachedItemsCount(store));

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    Assert.Equal(1, GetCachedItemsCount(store));

                    session.Load<Order>("orders/2");
                    Assert.Equal(2, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    order.CompanyId = "companies/2";
                    using (var bulk = store.BulkInsert(options: bulkOptions))
                        bulk.Store(order);

                    Assert.Equal(1, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    Assert.Equal(2, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/2");
                    order.CompanyId = "companies/2";
                    using (var bulk = store.BulkInsert(options: bulkOptions))
                        bulk.Store(order);

                    Assert.Equal(1, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/2");
                    Assert.Equal(2, GetCachedItemsCount(store));
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void cache_leftovers_after_document_delete(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(true, requestedStorage: requestedStorage))
            {
                store.DatabaseCommands.Admin.StopIndexing();
                Assert.Equal(0, GetCachedItemsCount(store));

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { CompanyId = "companies/1" });
                    session.SaveChanges();
                    // not in cache until we load the documents
                    Assert.Equal(0, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    Assert.Equal(1, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1");
                    session.SaveChanges();
                    Assert.Equal(0, GetCachedItemsCount(store));
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void cache_leftovers_after_documents_delete(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(true, requestedStorage: requestedStorage))
            {
                store.DatabaseCommands.Admin.StopIndexing();
                Assert.Equal(0, GetCachedItemsCount(store));

                using (var session = store.OpenSession())
                {
                    session.Store(new Order { CompanyId = "companies/1" });
                    session.Store(new Order { CompanyId = "companies/2" });
                    session.SaveChanges();
                    // not in cache until we load the documents
                    Assert.Equal(0, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Load<Order>("orders/1");
                    session.Load<Order>("orders/2");
                    Assert.Equal(2, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/1");
                    session.SaveChanges();
                    Assert.Equal(1, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Delete("orders/2");
                    session.SaveChanges();
                    Assert.Equal(0, GetCachedItemsCount(store));
                }
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void touch_document(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(true, requestedStorage: requestedStorage))
            {
                Assert.Equal(0, GetCachedItemsCount(store));

                store.DatabaseCommands.PutIndex("test", new IndexDefinition
                {
                    Map = @"
                        from i in docs.Items
                        select new
                        {
                            RefName = LoadDocument(i.Ref).Name,
                        }"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "1" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Load<Item>("items/2");
                    Assert.Equal(1, GetCachedItemsCount(store));
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "2" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                Assert.Equal(0, GetCachedItemsCount(store));
            }
        }

        [Theory]
        [PropertyData("Storages")]
        public void cache_leftovers_after_path_load_index(string requestedStorage)
        {
            using (var store = NewRemoteDocumentStore(true, requestedStorage: requestedStorage))
            {
                Assert.Equal(0, GetCachedItemsCount(store));

                store.DatabaseCommands.PutIndex("test", new IndexDefinition
                {
                    Map = @"
                        from i in docs.Items
                        select new
                        {
                            RefName = LoadDocument(i.Ref).Name,
                        }"
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "1" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                Assert.Equal(0, GetCachedItemsCount(store));

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "2" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                Assert.Equal(0, GetCachedItemsCount(store));

                using (var session = store.OpenSession())
                {
                    session.Load<Item>("items/2");
                    Assert.Equal(1, GetCachedItemsCount(store));
                }
            }
        }

        public class Item
        {
            public string Id { get; set; }
            public string Ref { get; set; }
            public string Name { get; set; }
        }

        public class Order
        {
            public string Id { get; set; }
            public string CompanyId { get; set; }
        }
    }
}
