using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.NestedIndexing
{
	public class CanIndexReferencedEntity : RavenTest
	{
		protected override void CreateDefaultIndexes(Client.IDocumentStore documentStore)
		{
		}

		[Fact]
		public void Simple()
		{
			using (var store = NewDocumentStore())
			{
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

		[Fact]
		public void WhenReferencedItemChanges()
		{
			using (var store = NewDocumentStore())
			{
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
					session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					session.Load<Item>(2).Name = "Arava";
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

		[Fact]
		public void WhenReferencedItemChangesInBatch()
		{
			using (var store = NewDocumentStore())
			{
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
					session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					session.Load<Item>(2).Name = "Arava";
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

		[Fact]
		public void WhenReferencedItemDeleted()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = @"
						from i in docs.Items
						select new
						{
							RefNameNotNull = LoadDocument(i.Ref).Name != null
						}"
				});

				using (var session = store.OpenSession())
				{
					session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
					session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					session.Delete(session.Load<Item>(2));
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

		[Fact]
		public void NightOfTheLivingDead()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = @"
						from i in docs.Items
						select new
						{
							RefName = LoadDocument(i.Ref).Name 
						}"
				});

				using (var session = store.OpenSession())
				{
					session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
					session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					session.Delete(session.Load<Item>(2));
					session.SaveChanges();
				}
				WaitForIndexing(store);

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

		[Fact]
		public void SelfReferencing()
		{
			using (var store = NewDocumentStore())
			{
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
					session.Store(new Item { Id = "items/1", Ref = "items/1", Name = "oren" });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					session.Load<Item>(1).Name = "Ayende";
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

		[Fact]
		public void Loops()
		{
			using (var store = NewDocumentStore())
			{
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
					session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "Oren" });
					session.Store(new Item { Id = "items/2", Ref = "items/1", Name = "Rahien" });
					session.SaveChanges();
				}

				WaitForIndexing(store);

				using (var session = store.OpenSession())
				{
					session.Load<Item>(2).Name = "Ayende";
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