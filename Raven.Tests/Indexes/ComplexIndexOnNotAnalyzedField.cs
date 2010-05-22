using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class ComplexIndexOnNotAnalyzedField: AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public ComplexIndexOnNotAnalyzedField()
		{
			db = new DocumentDatabase(new RavenConfiguration
			{
				DataDirectory = "raven.db.test.esent",
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true
			});
			db.SpinBackgroundWorkers();
		}

		#region IDisposable Members

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		#endregion

		[Fact]
		public void CanQueryOnKey()
		{
			db.Put("companies/", null,
			       JObject.Parse("{'Name':'Hiberanting Rhinos', 'Partners': ['companies/49', 'companies/50']}"), 
				   JObject.Parse("{'X-Raven-Entity-Name': 'Companies'}"),
			       null);


			db.PutIndex("CompaniesByPartners", new IndexDefinition
			{
				Map = "from company in docs.Companies from partner in company.Partners select new { Partner = partner }",
			});

			QueryResult queryResult;
			do
			{
				queryResult = db.Query("CompaniesByPartners", new IndexQuery
				{
					Query = "Partner:companies/49",
					PageSize = 10
				});
			} while (queryResult.IsStale);

			Assert.Equal("Hiberanting Rhinos", queryResult.Results[0].Value<string>("Name"));
		}
	}
}