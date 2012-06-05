// -----------------------------------------------------------------------
//  <copyright file="RavenDB_302.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Tests.Bugs;
using Xunit;
using System.Linq;

namespace Raven.Tests.Issues
{
	public class RavenDB_302 : RavenTest
	{
		[Fact]
		public void CanQueryUsingDefaultField()
		{
			using(var s = NewDocumentStore())
			{
				using (var session = s.OpenSession())
				{
					session.Store(new Item{Version = "first"});
					session.Store(new Item { Version = "second" });
					session.SaveChanges();
				}
				using(var session = s.OpenSession())
				{
					var x = session.Advanced.LuceneQuery<Item>()
						.WaitForNonStaleResults()
						.UsingDefaultField("Version")
						.Where("First OR Second")
						.ToList();

					Assert.Equal(2, x.Count);
				}
			}
		}

		[Fact]
		public void CanQueryUsingDefaultField_Remote()
		{
			using(GetNewServer())
			using (var s = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				s.DatabaseCommands.PutIndex("items_by_ver", new IndexDefinition
				{
					Map = "from doc in docs.Items select new { doc.Version }"
				});
				using (var session = s.OpenSession())
				{
					session.Store(new Item { Version = "first" });
					session.Store(new Item { Version = "second" });
					session.SaveChanges();
				}
				using (var session = s.OpenSession())
				{
					var x = session.Advanced.LuceneQuery<Item>("items_by_ver")
						.WaitForNonStaleResults()
						.UsingDefaultField("Version")
						.Where("First OR Second")
						.ToList();

					Assert.Equal(2, x.Count);
				}
			}
		}
	}
}