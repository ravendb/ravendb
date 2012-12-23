// -----------------------------------------------------------------------
//  <copyright file="CanTrackWhatCameFromWhat.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Indexing;
using Xunit;

namespace Raven.Tests.NestedIndexing
{
	public class CanTrackWhatCameFromWhat : RavenTest
	{
		public class Item
		{
			public string Id { get; set; }
			public string Ref { get; set; }
			public string Name { get; set; }
		}
		[Fact]
		public void SimpleIndex()
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
	Name = i.Name
}"
				});

				using (var session = store.OpenSession())
				{
					session.Store(new Item { Id = "items/1", Ref = "items/2", Name = "oren" });
					session.Store(new Item { Id = "items/2", Ref = "items/1", Name = "ayende" });
					session.SaveChanges();
				}

				WaitForIndexing(store);
			}
		}
	}
}