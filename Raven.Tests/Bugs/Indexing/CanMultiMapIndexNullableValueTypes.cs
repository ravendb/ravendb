using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit;
using Raven.Client.Indexes;
using Raven.Abstractions.Indexing;

namespace Raven.Tests.Bugs.Indexing
{
	public class CanMultiMapIndexNullableValueTypes : RavenTest
	{
		class Company
		{
			public decimal? Turnover { get; set; }
		}

		class Companies_ByTurnover : AbstractMultiMapIndexCreationTask
		{
			public Companies_ByTurnover()
			{
				AddMap<Company>(companies => from c in companies
											 select new
											 {
												 c.Turnover
											 });
			}
		}

		[Fact]
		public void WillNotProduceAnyErrors()
		{
			using (var store = NewDocumentStore(requestedStorage: "esent"))
			{
				var indexCreationTask = new Companies_ByTurnover();
				indexCreationTask.Execute(store);

				using (var s = store.OpenSession())
				{
					s.Store(new Company { Turnover = null });
					s.Store(new Company { Turnover = 1 });
					s.Store(new Company { Turnover = 2 });
					s.Store(new Company { Turnover = 3 });
					s.SaveChanges();
				}

				using (var s = store.OpenSession())
				{
					var results = s.Query<Company, Companies_ByTurnover>()
						.Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(1)))
						.ToArray();

					Assert.Equal(results.Length, 4);
				}

				Assert.Empty(store.DocumentDatabase.Statistics.Errors);
			}
		}
	}
}
