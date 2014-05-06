using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Document;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class TomCabanski : RavenTest
	{
		[Fact]
		public void CanEscapeGetFacets()
		{
			using (GetNewServer())
			using (var store = new DocumentStore
			{
				Url = "http://localhost:8079"
			}.Initialize())
			{
				store.DatabaseCommands.PutIndex("test", new IndexDefinition
				{
					Map = "from doc in docs select new { doc.Age, doc.IsActive, doc.BookVendor }"
				});

				using (var s = store.OpenSession())
				{
					s.Store(new FacetSetup
					{
						Id = "facets/test",
						Facets =
							{
								new Facet
								{
									Mode = FacetMode.Default,
									Name = "Age"
								}
							}
					});
					s.SaveChanges();
				}

				store.DatabaseCommands.GetFacets("test", new IndexQuery
				{
					Query = "(IsActive:true)  AND (BookVendor:\"stroheim & romann\")"
				}, "facets/test");
			}
		}
	}
}