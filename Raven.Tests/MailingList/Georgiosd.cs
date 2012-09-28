using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Georgiosd : RavenTest
	{
		[Fact]
		public void CanGet304FromLazyFacets()
		{
			using(var store = NewRemoteDocumentStore())
			{
				store.ExecuteIndex(new OrgIndex());

				using (var session = store.OpenSession())
				{
					var sector1 = new Sector { Id = 1, Name = "sector1" };
					var sector2 = new Sector { Id = 2, Name = "sector2" };

					for (int i = 0; i < 10; ++i)
					{
						session.Store(new Org { Id = i + 1, Sectors = new List<Sector> { i % 2 == 0 ? sector1 : sector2 } });
					}

					session.Store(new LocalFacet());
					session.SaveChanges();

					Consume(session.Query<Org, OrgIndex>()
						        .Customize(c => c.WaitForNonStaleResults())
						        .ToList());


					for (int i = 0; i < 5; i++)
					{
						var facetResults = session.Query<Org, OrgIndex>().Customize(c => c.WaitForNonStaleResults()).ToFacetsLazy(LocalFacet.Reference).Value;
						var facetResult = facetResults.Results["Sectors_Id"];
						Assert.Equal(2, facetResult.Values.Count);

						Assert.Equal("1", facetResult.Values[0].Range);
						Assert.Equal("2", facetResult.Values[1].Range);

						Assert.Equal(5, facetResult.Values[0].Hits);
						Assert.Equal(5, facetResult.Values[1].Hits);
					}
				}    
			}
		}
		public class Sector
		{
			public int Id { get; set; }
			public string Name { get; set; }
		}

		public class Org
		{
			public int Id { get; set; }
			public IList<Sector> Sectors { get; set; }
		}

		public class OrgIndex : AbstractIndexCreationTask<Org>
		{
			public OrgIndex()
			{
				Map = orgs => orgs.Select(org => new
				{
					Sectors_Id = org.Sectors.Select(s => s.Id)
				});
			}
		}

		private class LocalFacet : FacetSetup
		{
			public const string Reference = "facets/TestFacet";

			public LocalFacet()
			{
				Id = Reference;
				Facets = new List<Facet>
				{
					new Facet {Name = "Sectors_Id"}
				};
			}
		}
	}
}