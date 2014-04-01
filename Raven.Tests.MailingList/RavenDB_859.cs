using System.Linq;

using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	/// <summary>
	/// Demonstrates that "Id" can't be used in the where clause of a map/reduce query.
	/// Fails with an ArgumentException of:
	/// 
	///     The field '__document_id' is not indexed, cannot query on fields that are not indexed
	/// 
	/// Passes if you don't use it in a where clause, or if you call the reduce field something other than "Id".
	/// </summary>
	public class RavenDB_859 : RavenTestBase
	{
		public class Foo
		{
			public string Id { get; set; }
			public Bar[] Bars { get; set; }
		}

		public class Bar
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class OuterResult
		{
			public string Id { get; set; }
			public int Count { get; set; }
		}

		public class TestIndex : AbstractIndexCreationTask<Foo, TestIndex.Result>
		{
			public class Result
			{
				public string Id { get; set; }
				public int Count { get; set; }
			}

			public TestIndex()
			{
				Map = foos => from foo in foos
							  from bar in foo.Bars
							  select new
							  {
								  bar.Id,
								  Count = 1
							  };

				Reduce = results => from result in results
									group result by result.Id
										into g
										select new
										{
											Id = g.Key,
											Count = g.Sum(x => x.Count)
										};
			}
		}

		[Fact]
		public void NestedType()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new TestIndex());

				using (var session = documentStore.OpenSession())
				{
					var bar1 = new Bar { Name = "A" };
					var bar2 = new Bar { Name = "B" };
					var bar3 = new Bar { Name = "C" };

					session.Store(bar1);
					session.Store(bar2);
					session.Store(bar3);

					session.Store(new Foo { Bars = new[] { bar1, bar2, bar3 } });
					session.Store(new Foo { Bars = new[] { bar1, bar2, bar3 } });
					session.Store(new Foo { Bars = new[] { bar1, bar2, bar3 } });
					session.Store(new Foo { Bars = new[] { bar1, bar2, bar3 } });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<TestIndex.Result, TestIndex>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .Where(x => x.Id == "bars/1")
										 .ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal("bars/1", results[0].Id);
					Assert.Equal(4, results[0].Count);
				}
			}
		}

		[Fact]
		public void OuterType()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new TestIndex());

				using (var session = documentStore.OpenSession())
				{
					var bar1 = new Bar { Name = "A" };
					var bar2 = new Bar { Name = "B" };
					var bar3 = new Bar { Name = "C" };

					session.Store(bar1);
					session.Store(bar2);
					session.Store(bar3);

					session.Store(new Foo { Bars = new[] { bar1, bar2, bar3 } });
					session.Store(new Foo { Bars = new[] { bar1, bar2, bar3 } });
					session.Store(new Foo { Bars = new[] { bar1, bar2, bar3 } });
					session.Store(new Foo { Bars = new[] { bar1, bar2, bar3 } });

					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var results = session.Query<OuterResult, TestIndex>()
										 .Customize(x => x.WaitForNonStaleResults())
										 .Where(x => x.Id == "bars/1")
										 .ToList();

					Assert.Equal(1, results.Count);
					Assert.Equal("bars/1", results[0].Id);
					Assert.Equal(4, results[0].Count);
				}
			}
		}
	}
}