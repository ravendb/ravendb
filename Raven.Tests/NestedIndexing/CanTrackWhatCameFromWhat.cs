// -----------------------------------------------------------------------
//  <copyright file="CanTrackWhatCameFromWhat.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Xunit;

namespace Raven.Tests.NestedIndexing
{
	public class CanTrackWhatCameFromWhat : RavenTest
	{
		private readonly EmbeddableDocumentStore store;

		public CanTrackWhatCameFromWhat()
		{
			store = NewDocumentStore(requestedStorage: "esent");
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
		public void CrossRefrencing()
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
				Assert.Equal("items/2", accessor.Indexing.GetDocumentReferences("items/1").Single());
				Assert.Equal("items/1", accessor.Indexing.GetDocumentReferences("items/2").Single());
			});
		}

//		[Fact]
//		public void UpdatingDocument()
//		{
//			Assert.False(true);
//		}

//		[Fact]
//		public void UpdatingReferenceToAnotherDoc()
//		{
//			Assert.False(true);
//		}

//		[Fact]
//		public void UpdatingReferenceToNull()
//		{
//			Assert.False(true);
//		}

//		[Fact]
//		public void AddingReferenceToSamedoc()
//		{
//			Assert.False(true);
//		}

//		[Fact]
//		public void AddingReferenceToAnotherdoc()
//		{
//			Assert.False(true);
//		}

//		[Fact]
//		public void DeletingRefDoc()
//		{
//			Assert.False(true);
//		}

//		[Fact]
//		public void DeletingRootDoc()
//		{
//			Assert.False(true);
//		}
	}
}