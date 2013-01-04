using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Bugs.Chripede
{
	public class IndexOnList : RavenTest
	{
		[Fact]
		public void CanIndexAndQueryOnList()
		{
			using (var store = NewDocumentStore())
			{
				var container = new CompositionContainer(new TypeCatalog(typeof(Document_Index)));
				IndexCreation.CreateIndexes(container, store);

				using (var session = store.OpenSession())
				{
					session.Store(new Document
									  {
										  List = new List<string>() { "test1", "test2", "test3" }
									  });

					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var result = session.Query<Document, Document_Index>()
						.Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
						.Where(x => x.List.Any(s => s == "test1"))
						.ToList();

					Assert.Equal(1, result.Count);
				}

				//// Works when not using the index
				//using (var session = store.OpenSession())
				//{
				//    var result = session.Query<Document>()
				//        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
				//        .Where(x => x.List.Any(s => s == "test1"))
				//        .ToList();

				//    Assert.Equal(1, result.Count);
				//}
			}
		}
	}

	public class Document
	{
		public string Id { get; set; }

		public IList<string> List { get; set; }
	}

	public class Document_Index : AbstractIndexCreationTask<Document>
	{
		public Document_Index()
		{
			Map = docs => from doc in docs
						  select new
									 {
										 doc.List,
									 };
		}
	}
}
