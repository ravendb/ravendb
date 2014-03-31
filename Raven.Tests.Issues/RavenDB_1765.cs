// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1765 .cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_1765 : RavenTest
	{
		public class Foo
		{
			public string Id { get; set; }
			public DateTime DateTime { get; set; }
			public int Number { get; set; }
		}

		[Fact]
		public void HalfOpenRangeQueriesShouldWorkWithLuceneQuery()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Id = "1", DateTime = new DateTime(2014, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
					session.Store(new Foo { Id = "2", DateTime = new DateTime(2014, 1, 2, 0, 0, 0, DateTimeKind.Utc) });
					session.Store(new Foo { Id = "3", DateTime = new DateTime(2014, 1, 3, 0, 0, 0, DateTimeKind.Utc) });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var query = session.Advanced.LuceneQuery<Foo>()
						.Where("DateTime:[2014-01-01T00:00:00.0000000Z TO 2014-01-03T00:00:00.0000000Z}")
						.WaitForNonStaleResults();

					var results = query.ToList();

					Assert.Equal(2, results.Count);
					Assert.NotNull(results.First(x => x.Id == "1"));
					Assert.NotNull(results.First(x => x.Id == "2"));

					query = session.Advanced.LuceneQuery<Foo>()
						.Where("DateTime:{2014-01-01T00:00:00.0000000Z TO 2014-01-03T00:00:00.0000000Z]")
						.WaitForNonStaleResults();

					results = query.ToList();

					Assert.Equal(2, results.Count);
					Assert.NotNull(results.First(x => x.Id == "2"));
					Assert.NotNull(results.First(x => x.Id == "3"));

					query = session.Advanced.LuceneQuery<Foo>()
						.Where("DateTime:{2014-01-01T00:00:00.0000000Z TO 2014-01-03T00:00:00.0000000Z] AND DateTime:[2014-01-01T00:00:00.0000000Z TO 2014-01-03T00:00:00.0000000Z}")
						.WaitForNonStaleResults();

					results = query.ToList();

					Assert.Equal(1, results.Count);
					Assert.NotNull(results.First(x => x.Id == "2"));

					query = session.Advanced.LuceneQuery<Foo>()
						.Where("DateTime:[2014-01-01T00:00:00.0000000Z TO 2014-01-03T00:00:00.0000000Z} AND DateTime:{2014-01-01T00:00:00.0000000Z TO 2014-01-03T00:00:00.0000000Z]")
						.WaitForNonStaleResults();

					results = query.ToList();

					Assert.Equal(1, results.Count);
					Assert.NotNull(results.First(x => x.Id == "2"));
				}
			}
		}

		[Fact]
		public void HalfOpenRangeQueriesCandWorkWithLinq()
		{
			using (var documentStore = NewDocumentStore())
			{
				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo { Id = "1", Number = 1 });
					session.Store(new Foo { Id = "2", Number = 2 });
					session.Store(new Foo { Id = "3", Number = 3 });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var query = session.Query<Foo>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Number >= 1 && x.Number < 3);

					var results = query.ToList();

					Assert.Equal(2, results.Count);
					Assert.NotNull(results.First(x => x.Id == "1"));
					Assert.NotNull(results.First(x => x.Id == "2"));

					query = session.Query<Foo>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Number > 1 && x.Number <= 3);

					results = query.ToList();

					Assert.Equal(2, results.Count);
					Assert.NotNull(results.First(x => x.Id == "2"));
					Assert.NotNull(results.First(x => x.Id == "3"));

					query = session.Query<Foo>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Number >= 1 && x.Number < 3 && x.Number > 1);

					results = query.ToList();

					Assert.Equal(1, results.Count);
					Assert.NotNull(results.First(x => x.Id == "2"));
				}
			}
		}
	}
}