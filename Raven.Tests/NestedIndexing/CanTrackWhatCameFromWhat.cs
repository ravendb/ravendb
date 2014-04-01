// -----------------------------------------------------------------------
//  <copyright file="CanTrackWhatCameFromWhat.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.NestedIndexing
{
	public class CanTrackWhatCameFromWhat : RavenTest
	{
		private readonly EmbeddableDocumentStore store;

		public CanTrackWhatCameFromWhat()
		{
			store = NewDocumentStore();
			store.DatabaseCommands.PutIndex("test", new IndexDefinition
			{
				Map = @"
from i in docs.Items
select new
{
	RefName = LoadDocument(i.Ref).Name,
	Name = i.Name
}"
			});

		}

		protected override void CreateDefaultIndexes(IDocumentStore documentStore)
		{
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CrossReferencing()
		{
			using (IDocumentSession session = store.OpenSession())
			{
				session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
				session.Store(new Item { Id = "items/2", Ref = "items/1", Name = "ayende" });
				session.SaveChanges();
			}

			WaitForIndexing(store);

			store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
			{
				Assert.Equal("items/2", accessor.Indexing.GetDocumentsReferencing("items/1").Single());
				Assert.Equal("items/1", accessor.Indexing.GetDocumentsReferencing("items/2").Single());
			});
		}

		[Fact]
		public void UpdatingDocument()
		{
			using (IDocumentSession session = store.OpenSession())
			{
				session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
				session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
				session.SaveChanges();
			}

			WaitForIndexing(store);
			store.DocumentDatabase.TransactionalStorage.Batch(accessor => 
				Assert.Equal("items/1", accessor.Indexing.GetDocumentsReferencing("items/2").Single()));

			using (IDocumentSession session = store.OpenSession())
			{
				session.Load<Item>(1).Name = "other";
				session.SaveChanges();
			}

			WaitForIndexing(store);

			store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
				Assert.Equal("items/1", accessor.Indexing.GetDocumentsReferencing("items/2").Single()));
		}

		[Fact]
		public void UpdatingReferenceToAnotherDoc()
		{
			using (IDocumentSession session = store.OpenSession())
			{
				session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
				session.Store(new Item { Id = "items/2", Ref = null, Name = "ayende" });
				session.Store(new Item { Id = "items/3", Ref = null, Name = "ayende" });
				session.SaveChanges();
			}

			WaitForIndexing(store);
			store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
			{
				Assert.Equal("items/1", accessor.Indexing.GetDocumentsReferencing("items/2").Single());
				Assert.Empty(accessor.Indexing.GetDocumentsReferencing("items/3"));
			});

			using (IDocumentSession session = store.OpenSession())
			{
				session.Load<Item>(1).Ref = "items/3";
				session.SaveChanges();
			}

			WaitForIndexing(store);

			store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
			{
				Assert.Empty(accessor.Indexing.GetDocumentsReferencing("items/2"));
				Assert.Equal("items/1", accessor.Indexing.GetDocumentsReferencing("items/3").Single());

			});
		}

		[Fact]
		public void UpdatingReferenceToMissing()
		{
			using (IDocumentSession session = store.OpenSession())
			{
				session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
				session.SaveChanges();
			}

			WaitForIndexing(store);
			store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
				Assert.Equal("items/1", accessor.Indexing.GetDocumentsReferencing("items/2").Single()));
		}

		[Fact]
		public void UpdatingReferenceToNull()
		{
			using (IDocumentSession session = store.OpenSession())
			{
				session.Store(new Item { Id = "items/1", Ref = null, Name = "oren" });
				session.SaveChanges();
			}

			WaitForIndexing(store);
			store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
				Assert.Empty(accessor.Indexing.GetDocumentsReferencesFrom("items/1")));
	
		}

		[Fact]
		public void AddingReferenceToSamedoc()
		{
			using (IDocumentSession session = store.OpenSession())
			{
				session.Store(new Item { Id = "items/1", Ref = "items/1", Name = "oren" });
				session.SaveChanges();
			}

			WaitForIndexing(store);
			store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
				Assert.Empty(accessor.Indexing.GetDocumentsReferencing("items/1")));
		}

		[Fact]
		public void DeletingRootDoc()
		{
			using (IDocumentSession session = store.OpenSession())
			{
				session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
				session.Store(new Item { Id = "items/2", Ref = null, Name = "oren" });
				session.SaveChanges();
			}

			WaitForIndexing(store);
			store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
				Assert.NotEmpty(accessor.Indexing.GetDocumentsReferencesFrom("items/1")));

			store.DatabaseCommands.Delete("items/1", null);
			store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
				Assert.Empty(accessor.Indexing.GetDocumentsReferencesFrom("items/1")));

		}
	}
}