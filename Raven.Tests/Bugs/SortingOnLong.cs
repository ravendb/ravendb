//-----------------------------------------------------------------------
// <copyright file="SortingOnLong.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class SortingOnLong : RavenTest
	{
		void UsingDatabaseOfFoos(Action<IDocumentSession> action)
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new Foo
					{
						Value = 30000000000
					});

					session.Store(new Foo
					{
						Value = 25
					});

					session.Store(new Foo
					{
						Value = 3147483647
					});

					session.SaveChanges();
				}

				store.DatabaseCommands.PutIndex("long",
												new IndexDefinition
												{
													Map = "from doc in docs select new { doc.Value }",
													SortOptions = { { "Value", SortOptions.Long } },
												});

				using (var session = store.OpenSession())
				{
					action(session);
				}
			}
		}

		[Fact]
		public void CanSortOnLong()
		{
			UsingDatabaseOfFoos(delegate(IDocumentSession session)
			{
				var foos1 = session.Advanced.LuceneQuery<Foo>("long")
					.WaitForNonStaleResults()
					.OrderBy("Value_Range")
					.ToList();

				Assert.Equal(3, foos1.Count);

				Assert.Equal(25, foos1[0].Value);
				Assert.Equal(3147483647, foos1[1].Value);
				Assert.Equal(30000000000, foos1[2].Value);
			});
		}

		[Fact]
		public void CanLinqSortOnLong()
		{
			UsingDatabaseOfFoos(delegate(IDocumentSession session)
			{
				var foos1 = session.Query<Foo>("long")
					.Customize(q => q.WaitForNonStaleResults())
					.OrderBy(f => f.Value)
					.ToList();

				Assert.Equal(3, foos1.Count);

				Assert.Equal(25, foos1[0].Value);
				Assert.Equal(3147483647, foos1[1].Value);
				Assert.Equal(30000000000, foos1[2].Value);
			});
		}

		[Fact]
		public void CanSortOnLongDescending()
		{
			UsingDatabaseOfFoos(delegate(IDocumentSession session)
			{
				var foos1 = session.Advanced.LuceneQuery<Foo>("long")
					.WaitForNonStaleResults()
					.OrderBy("-Value_Range")
					.ToList();

				Assert.Equal(3, foos1.Count);

				Assert.Equal(30000000000, foos1[0].Value);
				Assert.Equal(3147483647, foos1[1].Value);
				Assert.Equal(25, foos1[2].Value);
			});
		}

		[Fact]
		public void CanLinqSortOnLongDescending()
		{
			UsingDatabaseOfFoos(delegate(IDocumentSession session)
			{
				var foos1 = session.Query<Foo>("long")
					.Customize(q => q.WaitForNonStaleResults())
					.OrderByDescending(f => f.Value)
					.ToList();

				Assert.Equal(3, foos1.Count);

				Assert.Equal(30000000000, foos1[0].Value);
				Assert.Equal(3147483647, foos1[1].Value);
				Assert.Equal(25, foos1[2].Value);
			});
		}
		
		public class Foo
		{
			public string Id { get; set; }
			public long Value { get; set; }

			public override string ToString()
			{
				return string.Format("Id: {0}, Value: {1}", Id, Value);
			}
		}
	}
}
