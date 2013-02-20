// -----------------------------------------------------------------------
//  <copyright file="RavenDB_790.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_790 : RavenTest
	{
		private class Item
		{
			public string Id { get; set; }
			public string Name { get; set; } 
		}

		[Fact]
		public void CanDisableQueryResultsTrackingForDocumentSessionQuery()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Id = "items/1",
						Name = "abc"
					});

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<Item>().Customize(x => x.NoTracking().WaitForNonStaleResults()).FirstOrDefault();
					Assert.NotNull(result);

					// TODO arek
				}
			}
		}

		[Fact]
		public void CanDisableQueryResultsTrackingForAsyncDocumentSessionQuery()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Item
					{
						Id = "items/1",
						Name = "abc"
					});

					session.SaveChanges();
				}

				using (var asyncSession = store.OpenAsyncSession())
				{
					var queryResultAsync = asyncSession.Query<Item>().Customize(x => x.NoTracking()).ToListAsync();

					queryResultAsync.Wait();
				}
			}
		}
	}
}