using System.Linq;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class StaticDynamic : RavenTest
	{
		[Fact]
		public void IfStaticQueryHasWhere_SeparateDynamicQueryCreated()
		{
			using (var store = NewDocumentStore())
			{
				new Docs_Flagged().Execute(store);
				const int docsCount = 10;

				CreateDocs(store, docsCount);

				using (var session = store.OpenSession())
				{
					var docsByDynamicIndex = session.Advanced.LuceneQuery<TestDoc>().WaitForNonStaleResults().ToList();
					Assert.Equal(docsCount, docsByDynamicIndex.Count);
				}

				using (var session = store.OpenSession())
				{
					var docsByStaticIndex = session.Advanced.LuceneQuery<TestDoc, Docs_Flagged>().WaitForNonStaleResults().ToList();
					Assert.Equal(docsCount/2, docsByStaticIndex.Count);
				}
			}
		}

		[Fact]
		public void IfNoStaticQuery_SeparateDynamicQueryCreated()
		{
			using (var store = NewDocumentStore())
			{
				const int docsCount = 10;
				CreateDocs(store, docsCount);

				var session = store.OpenSession();

				var docsByDynamicIndex = session.Advanced.LuceneQuery<TestDoc>().WaitForNonStaleResults().ToList();
				Assert.Equal(docsCount, docsByDynamicIndex.Count);
			}
		}

		private static void CreateDocs(IDocumentStore store, int docsCount)
		{
			using (var session = store.OpenSession())
			{
				for (var i = 0; i < docsCount; i++)
				{
					session.Store(new TestDoc {Flag = i%2 == 0});
				}
				session.SaveChanges();
			}
		}
	}

	public class Docs_Flagged : AbstractIndexCreationTask<TestDoc>
	{
		public Docs_Flagged()
		{
			Map = testDocs => from doc in testDocs
							  where doc.Flag
							  select new { doc.Id };
		}
	}

	public class TestDoc
	{
		public string Id { get; set; }
		public bool Flag { get; set; }
	}
}
