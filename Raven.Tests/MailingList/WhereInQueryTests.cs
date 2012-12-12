using Raven.Tests;

namespace ContainsQueryFail
{
	using System;
	using System.Collections.Generic;
	using System.Linq;
	using Raven.Client;
	using Raven.Client.Embedded;
	using Raven.Client.Indexes;
	using Xunit;

	public class WhereInQueryTests : RavenTest
	{
		private readonly EmbeddableDocumentStore _documentStore;

		public WhereInQueryTests()
		{
			_documentStore = NewDocumentStore();

			using (IDocumentSession session = _documentStore.OpenSession())
			{
				session.Store(new TestDocument
							  {
								  Id = "Doc1",
								  FirstName = "FirstName1",
								  LastName = "LastName1"
							  });
				session.Store(new TestDocument
							  {
								  Id = "Doc2",
								  FirstName = "FirstName2",
								  LastName = "LastName2"
							  });
				session.Store(new TestDocument
							  {
								  Id = "Doc3",
								  FirstName = "FirstName3",
								  LastName = "LastName3"
							  });
				session.SaveChanges();
			}

			new TestIndex().Execute(_documentStore);
		}

		public override void Dispose()
		{
			_documentStore.Dispose();
			base.Dispose();
		}

		[Fact]
		public void Query_should_return_2_results()
		{
			using (IDocumentSession session = _documentStore.OpenSession())
			{
				List<TestDocument> results = session
					.Advanced
					.LuceneQuery<TestDocument, TestIndex>()
					.WhereEquals("FirstName", "FirstName1")
					.OrElse()
					.WhereEquals("LastName", "LastName2")
					.WaitForNonStaleResults()
					.ToList();

				Assert.Equal(2, results.Count);
			}
		}

		[Fact]
		public void Query_using_WhereIn_should_return_2_results()
		{
			using (IDocumentSession session = _documentStore.OpenSession())
			{
				List<TestDocument> results = session
					.Advanced
					.LuceneQuery<TestDocument, TestIndex>()
					.WhereIn("FirstName", new[] { "FirstName1" })
					.OrElse()
					.WhereIn("LastName", new[] { "LastName2" })
					.WaitForNonStaleResults()
					.ToList();

				Assert.Equal(2, results.Count);
			}
		}

		public class TestDocument
		{
			public string FirstName { get; set; }

			public string Id { get; set; }

			public string LastName { get; set; }
		}

		public class TestIndex : AbstractIndexCreationTask<TestDocument>
		{
			public TestIndex()
			{
				Map = docs => from doc in docs
							  select new
									 {
										 doc.FirstName,
										 doc.LastName
									 };
			}
		}
	}
}