// -----------------------------------------------------------------------
//  <copyright file="LoadAllStartingWith.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class LoadAllStartingWith : RavenTest
	{
		private class Item
		{
			public string Id { get; set; }
		}

		[Fact]
		public void LoadAllStartingWithShouldNotLoadDeletedDocs()
		{
			using (var store = NewRemoteDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item {Id = "doc/1"});
					session.Store(new Item {Id = "doc/2"});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					// commenting out this call passes the test
					var items = session.Advanced.LoadStartingWith<Item>("doc/").ToList();

					var item1 = session.Load<Item>("doc/1");
					session.Delete(item1);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var item1 = session.Load<Item>("doc/1");
					Assert.Null(item1);

					var items = session.Advanced.LoadStartingWith<Item>("doc/").ToList();
					Assert.Equal("doc/2", items.Single().Id);
				}
			}
		}
	}
}