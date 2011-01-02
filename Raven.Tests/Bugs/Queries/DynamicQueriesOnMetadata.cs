using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Data;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs.Queries
{
	public class DynamicQueriesOnMetadata : LocalClientTest
	{
		[Fact]
		public void CanQueryOnMetadataUsingDynamicQueries()
		{
			using(var store = NewDocumentStore())
			{
				using(var s = store.OpenSession())
				{
					var g = new Glass();
					s.Store(g);
					s.Advanced.GetMetadataFor(g)["Is-Nice"] = true;
					s.SaveChanges();
				}


				using (var s = store.OpenSession())
				{
					var glasses = s.Advanced.LuceneQuery<Glass>()
						.WhereEquals("@metadata.Is-Nice", true)
						.ToArray();
					Assert.NotEmpty(glasses);
				}
			}
		}

		public class Glass
		{
			public string Id { get; set; }
		}

		[Fact]
		public void WillGenerateProperQueryForMetadata()
		{
			var mapping = DynamicQueryMapping.Create(new DocumentDatabase(new RavenConfiguration { RunInMemory = true }), "@metadata.Raven-Graph-Type:Edge", null);

			var indexDefinition = mapping.CreateIndexDefinition();

			Assert.Equal(@"from doc in docs
select new { metadataRavenGraphType = doc[""@metadata""][""Raven-Graph-Type""] }", indexDefinition.Map);
		}
	}
}