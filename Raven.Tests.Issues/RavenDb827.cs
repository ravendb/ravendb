using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDb827 : RavenTestBase
	{
		public class TranTest
		{
			public string Id { get; set; }
			public IDictionary<string, string> Trans { get; set; }
		}

		public class TranTestIndex : AbstractIndexCreationTask<TranTest>
		{
			public TranTestIndex()
			{
				Map = docs =>
					  from doc in docs
					  select new
					  {
						  _ = doc.Trans.Select(x => CreateField("Trans_" + x.Key, x.Value)),
					  };

				Index("Trans_en", FieldIndexing.Analyzed);
				Index("Trans_fi", FieldIndexing.Analyzed);
			}
		}

		[Fact]
		public void Can_Use_Dictionary_Created_Field_In_Lucene_Search()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new TranTestIndex());

				using (var session = documentStore.OpenSession())
				{
					session.Store(new TranTest { Trans = new Dictionary<string, string> { { "en", "abc" }, { "fi", "def" } } });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var searchTerms = "abc";

                    var query = session.Advanced.DocumentQuery<TranTest, TranTestIndex>()
									   .WaitForNonStaleResults()
									   .Search(x => x.Trans["en"], searchTerms);
					var results = query.ToList();

					Assert.Equal(1, results.Count);
				}
			}
		}

		[Fact]
		public void Can_Use_Dictionary_Created_Field_In_Linq_Where()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new TranTestIndex());

				using (var session = documentStore.OpenSession())
				{
					session.Store(new TranTest { Trans = new Dictionary<string, string> { { "en", "abc" }, { "fi", "def" } } });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var searchTerms = "abc";

					var query = session.Query<TranTest, TranTestIndex>()
									   .Customize(x => x.WaitForNonStaleResults())
									   .Where(x => x.Trans["en"].StartsWith(searchTerms));

					var results = query.ToList();

					Assert.Equal(1, results.Count);
				}
			}
		}

		[Fact]
		public void Can_Use_Dictionary_Created_Field_In_Linq_Search()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new TranTestIndex());

				using (var session = documentStore.OpenSession())
				{
					session.Store(new TranTest { Trans = new Dictionary<string, string> { { "en", "abc" }, { "fi", "def" } } });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var searchTerms = "abc";

					var query = session.Query<TranTest, TranTestIndex>()
									   .Customize(x => x.WaitForNonStaleResults())
									   .Search(x => x.Trans["en"], searchTerms);

					var results = query.ToList();

					Assert.Equal(1, results.Count);
				}
			}
		}

		[Fact]
		public void Can_Use_Dictionary_Created_Field_In_Linq_Search_Workaround()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.ExecuteIndex(new TranTestIndex());

				using (var session = documentStore.OpenSession())
				{
					session.Store(new TranTest { Trans = new Dictionary<string, string> { { "en", "abc" }, { "fi", "def" } } });
					session.SaveChanges();
				}

				using (var session = documentStore.OpenSession())
				{
					var searchTerms = "abc";

					var query = session.Query<TranTest, TranTestIndex>()
									   .Customize(x => x.WaitForNonStaleResults())
									   .Customize(x => ((IDocumentQuery<TranTest>)x).Search(q => q.Trans["en"], searchTerms));

					var results = query.ToList();

					Assert.Equal(1, results.Count);
				}
			}
		}
	}
}