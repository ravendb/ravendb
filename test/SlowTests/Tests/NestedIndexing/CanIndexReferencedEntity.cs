using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.NestedIndexing
{
    public class CanIndexReferencedEntity : RavenTestBase
    {
        public CanIndexReferencedEntity(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {
            public string Id { get; set; }
            public string Ref { get; set; }
            public string Name { get; set; }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Simple(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = {
                        @"
                        from i in docs.Items
                        select new
                        {
                            RefName = LoadDocument(i.Ref, ""Items"").Name,
                        }"
                    }, 
                    Name = "test" }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var item = session.Advanced.DocumentQuery<Item>("test")
                                      .WaitForNonStaleResults()
                                      .WhereEquals("RefName", "ayende")
                                      .Single();
                    Assert.Equal("items/1", item.Id);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void WhenReferencedItemChanges(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = {
                        @"
                        from i in docs.Items
                        select new
                        {
                            RefName = LoadDocument(i.Ref, ""Items"").Name,
                        }"
                    },
                    Name = "test" }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Load<Item>("items/2").Name = "Arava";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var item = session.Advanced.DocumentQuery<Item>("test")
                                      .WaitForNonStaleResults()
                                      .WhereEquals("RefName", "arava")
                                      .Single();
                    Assert.Equal("items/1", item.Id);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void WhenReferencedItemChangesInBatch(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = {
                        @"
                        from i in docs.Items
                        select new
                        {
                            RefName = LoadDocument(i.Ref, ""Items"").Name,
                        }"
                    },
                    Name = "test" }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Load<Item>("items/2").Name = "Arava";
                    session.Store(new Item { Id = "items/3", Ref = null, Name = "ayende" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var item = session.Advanced.DocumentQuery<Item>("test")
                                      .WaitForNonStaleResults()
                                      .WhereEquals("RefName", "arava")
                                      .Single();
                    Assert.Equal("items/1", item.Id);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void WhenReferencedItemDeleted(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = {
                        @"
                        from i in docs.Items
                        select new
                        {
                            RefNameNotNull = LoadDocument(i.Ref, ""Items"").Name != null
                        }"
                    },
                    Name = "test" }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Delete(session.Load<Item>("items/2"));
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var item = session.Advanced.DocumentQuery<Item>("test")
                                      .WaitForNonStaleResults()
                                      .WhereEquals("RefNameNotNull", false)
                                      .Single();
                    Assert.Equal("items/1", item.Id);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void NightOfTheLivingDead(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation( new [] {new IndexDefinition
                {
                    Maps = {
                        @"
                        from i in docs.Items
                        select new
                        {
                            RefName = LoadDocument(i.Ref, ""Items"").Name 
                        }"
                    },
                    Name = "test" }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Delete(session.Load<Item>("items/2"));
                    session.SaveChanges();
                }
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/2", Ref = null, Name = "Rahien" });
                    session.SaveChanges();
                }


                using (var session = store.OpenSession())
                {
                    var item = session.Advanced.DocumentQuery<Item>("test")
                                      .WaitForNonStaleResults()
                                      .WhereEquals("RefName", "Rahien")
                                      .Single();
                    Assert.Equal("items/1", item.Id);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void SelfReferencing(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = {
                        @"
                        from i in docs.Items
                        select new
                        {
                            RefName = LoadDocument(i.Ref, ""Items"").Name,
                        }"
                    },
                    Name = "test" }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/1", Name = "oren" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Load<Item>("items/1").Name = "Ayende";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var item = session.Advanced.DocumentQuery<Item>("test")
                                      .WaitForNonStaleResults()
                                      .WhereEquals("RefName", "Ayende")
                                      .Single();
                    Assert.Equal("items/1", item.Id);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Loops(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = {
                        @"
                        from i in docs.Items
                        select new
                        {
                            RefName = LoadDocument(i.Ref, ""Items"").Name,
                        }"
                    },
                    Name = "test" }
                }));

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "Oren" });
                    session.Store(new Item { Id = "items/2", Ref = "items/1", Name = "Rahien" });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Load<Item>("items/2").Name = "Ayende";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var item = session.Advanced.DocumentQuery<Item>("test")
                                      .WaitForNonStaleResults()
                                      .WhereEquals("RefName", "Ayende")
                                      .Single();
                    Assert.Equal("items/1", item.Id);
                }
            }
        }
    }
}
