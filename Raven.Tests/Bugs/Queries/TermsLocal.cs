//-----------------------------------------------------------------------
// <copyright file="TermsLocal.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Indexing;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Queries
{
	public class TermsLocal : RavenTest
	{
		[Fact]
		public void CanGetTermsForIndex()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					for (int i = 0; i < 15; i++)
					{
						s.Store(new User{ Name = "user"+i.ToString("000")});
					}
					s.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Name }"
												});

				using (var s = store.OpenSession())
				{
					s.Query<User>("test").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				var terms = store.DatabaseCommands.GetTerms("test", "Name", null, 10)
					.OrderBy(x=>x)
					.ToArray();

				Assert.Equal(10, terms.Count());

				for (int i = 0; i < 10; i++)
				{
					Assert.Equal("user"+i.ToString("000"), terms.ElementAt(i));
				}
			}
		}

		[Fact]
		public void CanGetTermsForIndex_WithPaging()
		{
			using (var store = NewDocumentStore())
			{
				using (var s = store.OpenSession())
				{
					for (int i = 0; i < 15; i++)
					{
						s.Store(new User { Name = "user" + i.ToString("000") });
					}
					s.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("test",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Name }"
												});

				using (var s = store.OpenSession())
				{
					s.Query<User>("test").Customize(x => x.WaitForNonStaleResults()).ToList();
				}

				var terms = store.DatabaseCommands.GetTerms("test", "Name", "user009", 10)
					.OrderBy(x => x)
					.ToArray();

				Assert.Equal(5, terms.Count());

				for (int i = 10; i < 15; i++)
				{
					Assert.Equal("user" + i.ToString("000"), terms.ElementAt(i - 10));
				}
			}
		}
	}
}
