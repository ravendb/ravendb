using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.MailingList
{
	public class gjerster : RavenTest
	{
		[Theory]
		[InlineData("singa*")]
		[InlineData("pte")]
		[InlineData("ltd")]
		[InlineData("*inga*")]
		public void CanSearchWithPrefixWildcard(string query)
		{
			using (var store = NewDocumentStore())
			{
				new SampleDataIndex().Execute(store);

				using (IDocumentSession session = store.OpenSession())
				{
					session.Store(new SampleData
					{
						Name = "Singapore",
						Description = "SINGAPORE PTE LTD"
					});

					session.SaveChanges();
				}

				using (IDocumentSession session = store.OpenSession())
				{
					var rq = session
						.Query<SampleDataIndex.ReducedResult, SampleDataIndex>()
						.Customize(customization => customization.WaitForNonStaleResultsAsOfNow());
					var result =
						rq.Search(x => x.Query, query,
						          escapeQueryOptions: EscapeQueryOptions.AllowAllWildcards)
							.As<SampleData>()
							.Take(10)
							.ToList();
					if(result.Count == 0)
					{
						
					}
					Assert.NotEmpty(result);
				}
			}
		}
	}

	public class SampleData
	{
		public string Name { get; set; }
		public string Description { get; set; }
	}

	public class SampleDataIndex : AbstractIndexCreationTask<SampleData, SampleDataIndex.ReducedResult>
	{
		public SampleDataIndex()
		{
			Map = docs => from doc in docs
			              select new
			              {
				              Query = new object[]
				              {
					              doc.Name,
					              doc.Description
				              }
			              };
			Indexes.Add(x => x.Query, FieldIndexing.Analyzed);
		}

		#region Nested type: ReducedResult

		public class ReducedResult
		{
			public string Query { get; set; }
		}

		#endregion
	}
}
