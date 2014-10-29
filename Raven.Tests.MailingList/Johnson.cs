using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Johnson : RavenTest
	{
		public class Foo
		{
			public string Id { get; set; }
			public List<Bar> Bars { get; set; }
		}

		public class Bar
		{
			public DateTime Date { get; set; }
			public decimal Amount { get; set; }
		}

		public class FoosTotalByBarDate : AbstractIndexCreationTask<Foo, FoosTotalByBarDate.ReduceResult>
		{
			public FoosTotalByBarDate()
			{
				Map = foos =>
				  from foo in foos
				  from bar in foo.Bars
				  select new
				  {
					  Date = bar.Date,
					  Total = bar.Amount
				  };

				Reduce = results =>
				  results.GroupBy(x => x.Date)
						 .Select(x => new
						 {
							 Date = x.Key,
							 Total = x.Sum(y => y.Total)
						 });
			}

			public class ReduceResult
			{
				public DateTime Date { get; set; }
				public decimal Total { get; set; }
			}
		}

		[Fact]
		public void CanGroupOnDate()
		{
			using (var documentStore = NewDocumentStore())
			{
				new FoosTotalByBarDate().Execute(documentStore);

				using (var session = documentStore.OpenSession())
				{
					var foo1 = new Foo
					{
						Bars = new List<Bar>
						{
							new Bar {Date = new DateTime(2011, 1, 1), Amount = 100},
							new Bar {Date = new DateTime(2011, 1, 2), Amount = 200},
							new Bar {Date = new DateTime(2011, 1, 3), Amount = 300},
						}
					};

					var foo2 = new Foo
					{
						Bars = new List<Bar>
						{
							new Bar {Date = new DateTime(2011, 1, 1), Amount = 100},
							new Bar {Date = new DateTime(2011, 1, 2), Amount = 200},
							new Bar {Date = new DateTime(2011, 1, 3), Amount = 300},
						}
					};

					session.Store(foo1);
					session.Store(foo2);
					session.SaveChanges();
				}


				using (var session = documentStore.OpenSession())
				{
					var reduceResults = session.Query<FoosTotalByBarDate.ReduceResult, FoosTotalByBarDate>()
						.Customize(x => x.WaitForNonStaleResults())
						.OrderBy(x=>x.Date)
						.ToList();

					Assert.Equal(3, reduceResults.Count);
					Assert.Equal(new DateTime(2011, 1, 1),reduceResults[0].Date);
					Assert.Equal(200, reduceResults[0].Total);
					Assert.Equal(new DateTime(2011, 1, 2), reduceResults[1].Date);
					Assert.Equal(400, reduceResults[1].Total);
					Assert.Equal(new DateTime(2011, 1, 3), reduceResults[2].Date);
					Assert.Equal(600, reduceResults[2].Total);
				}
			}
		}
	}
}