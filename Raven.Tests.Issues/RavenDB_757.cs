// -----------------------------------------------------------------------
//  <copyright file="RavenDB_757.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_757 : RavenTest
	{
		[Fact]
		public void Test()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.Initialize();

				PutIndex1(documentStore);
				PutIndex2(documentStore);

				documentStore.ExecuteIndex(new TestIndex3());
				documentStore.ExecuteIndex(new TestIndex4());

				using (var session = documentStore.OpenSession())
				{
					session.Store(new Foo
					{
						StartDate = new DateTime(2012, 1, 1),
						EndDate = new DateTime(2012, 1, 3)
					});

					session.SaveChanges();

					WaitForUserToContinueTheTest(documentStore);

					var q1 = session.Query<Result>("TestIndex1").Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Date).ToList();
					var q2 = session.Query<Result>("TestIndex2").Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Date).ToList();
					var q3 = session.Query<Result, TestIndex3>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Date).ToList();
					var q4 = session.Query<Result, TestIndex4>().Customize(x => x.WaitForNonStaleResults()).OrderBy(x => x.Date).ToList();

					Assert.Equal(3, q1.Count);
					Assert.Equal(new DateTime(2012, 1, 1), q1[0].Date);
					Assert.Equal(new DateTime(2012, 1, 2), q1[1].Date);
					Assert.Equal(new DateTime(2012, 1, 3), q1[2].Date);
					Assert.Equal(1, q1[0].Count);
					Assert.Equal(1, q1[1].Count);
					Assert.Equal(1, q1[2].Count);

					Assert.Equal(3, q2.Count);
					Assert.Equal(new DateTime(2012, 1, 1), q2[0].Date);
					Assert.Equal(new DateTime(2012, 1, 2), q2[1].Date);
					Assert.Equal(new DateTime(2012, 1, 3), q2[2].Date);
					Assert.Equal(1, q2[0].Count);
					Assert.Equal(1, q2[1].Count);
					Assert.Equal(1, q2[2].Count);

					Assert.Equal(3, q3.Count);
					Assert.Equal(new DateTime(2012, 1, 1), q3[0].Date);
					Assert.Equal(new DateTime(2012, 1, 2), q3[1].Date);
					Assert.Equal(new DateTime(2012, 1, 3), q3[2].Date);
					Assert.Equal(1, q3[0].Count);
					Assert.Equal(1, q3[1].Count);
					Assert.Equal(1, q3[2].Count);

					Assert.Equal(3, q4.Count);
					Assert.Equal(new DateTime(2012, 1, 1), q4[0].Date);
					Assert.Equal(new DateTime(2012, 1, 2), q4[1].Date);
					Assert.Equal(new DateTime(2012, 1, 3), q4[2].Date);
					Assert.Equal(1, q4[0].Count);
					Assert.Equal(1, q4[1].Count);
					Assert.Equal(1, q4[2].Count);
				}
			}
		}

		// For sake of simplifying these tests, the dates here are all whole, local dates, at midnight, and are fully inclusive.
		// For example, [2012-01-01T00:00:00 - 2012-01-03T00:00:00] = [2012-01-01T00:00:00, 2012-01-02T00:00:00, 2012-01-03T00:00:00]

		public class Foo
		{
			public DateTime StartDate { get; set; }
			public DateTime EndDate { get; set; }
		}

		public class Result
		{
			public DateTime Date { get; set; }
			public int Count { get; set; }
		}

		private static void PutIndex1(IDocumentStore documentStore)
		{
			var index = new IndexDefinition
			{
				Map = @"from foo in docs.Foos
                            from i in Enumerable.Range(0, (int) (foo.EndDate - foo.StartDate).TotalDays + 1)
                            select new
                                {
                                    Date = foo.StartDate.AddDays((int)i),
                                    Count = 1
                                }",
				Reduce = @"from result in results
                               group result by result.Date
                               into g
                               select new
                                   {
                                       Date = g.Key,
                                       Count = g.Sum(x => x.Count)
                                   }"
			};

			documentStore.DatabaseCommands.PutIndex("TestIndex1", index);
		}

		private static void PutIndex2(IDocumentStore documentStore)
		{
			var index = new IndexDefinition
			{
				Map = @"docs.Foos.SelectMany(doc => Enumerable.Range(0, (int)(doc.EndDate - doc.StartDate).TotalDays + 1).Cast<object>(), (doc, i) => new {
                            Date = doc.StartDate.AddDays(i),
                            Count = 1
                        })",
				Reduce = @"results.GroupBy(result => result.Date).Select(g => new {
                            Date = g.Key,
                            Count = Enumerable.Sum(g, x => ((int) x.Count))
                        })"
			};

			documentStore.DatabaseCommands.PutIndex("TestIndex2", index);
		}

		public class TestIndex3 : AbstractIndexCreationTask<Foo, Result>
		{
			public TestIndex3()
			{
				Map = foos => from foo in foos
							  from i in Enumerable.Range(0, (int)(foo.EndDate - foo.StartDate).TotalDays + 1)
							  select new
							  {
								  Date = foo.StartDate.AddDays(i),
								  Count = 1
							  };

				Reduce = results => from result in results
									group result by result.Date
										into g
										select new
										{
											Date = g.Key,
											Count = g.Sum(x => x.Count)
										};
			}
		}

		public class TestIndex4 : AbstractIndexCreationTask<Foo, Result>
		{
			public TestIndex4()
			{
				// You can't cast to object here even if you wanted to. The <object> gets lost in translation.
				//Map = foos => foos.SelectMany(doc => Enumerable.Range(0, (int) (doc.EndDate - doc.StartDate).TotalDays + 1).Cast<object>(),
				Map = foos => foos.SelectMany(doc => Enumerable.Range(0, (int)(doc.EndDate - doc.StartDate).TotalDays + 1),
											  (doc, i) => new
											  {
												  Date = doc.StartDate.AddDays((int)i),
												  Count = 1
											  });

				Reduce = results => results.GroupBy(result => result.Date).Select(g => new
				{
					Date = g.Key,
					Count = g.Sum(x => x.Count)
				});
			}
		} 
	}
}