using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Querying
{
	public class SearchOperator : RavenTest
	{
		public class Something
		{
			public int Id { get; set; }
			public string MyProp { get; set; }
		}

		public class FTSIndex : AbstractIndexCreationTask<Something>
		{
			public FTSIndex()
			{
				Map = docs => from doc in docs
				              select new {doc.MyProp};

				Indexes.Add(x => x.MyProp, FieldIndexing.Analyzed);
			}
		}

		[Fact]
		public void DynamicLuceneQuery()
		{
			using (var store = NewDocumentStore())
			{
				new FTSIndex().Execute(store);

				using (var session = store.OpenSession())
				{
					// insert two test documents
					session.Store(new Something {Id = 23, MyProp = "the first string contains misspelled word sofware"});
					session.Store(new Something {Id = 34, MyProp = "the second string contains the word software"});
					session.SaveChanges();

					// search for the keyword software
					var results = session.Advanced.LuceneQuery<Something>("FTSIndex").Search("MyProp", "software")
						.WaitForNonStaleResultsAsOfLastWrite()
						.ToList();
					Assert.Equal(1, results.Count);

					results = session.Advanced.LuceneQuery<Something>("FTSIndex").Search("MyProp", "software~")
						.WaitForNonStaleResultsAsOfLastWrite().ToList();
					Assert.Equal(2, results.Count);
				}
			}
		}
	}
}
