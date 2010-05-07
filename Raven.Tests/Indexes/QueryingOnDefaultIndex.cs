using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Data;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Indexes
{
	public class QueryingOnDefaultIndex: AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public QueryingOnDefaultIndex()
		{
			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true});
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
		public void CanQueryOverDefaultIndex()
		{
			db.Put("users/ayende", null, JObject.Parse("{'email':'ayende@ayende.com'"),
			       JObject.Parse("{'Raven-Entity-Name': 'Users'}"), null);

			QueryResult queryResult;
			do
			{
				queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:`Users`",
					PageSize = 10
				});
			} while (queryResult.IsStale);

			Assert.Equal("ayende@ayende.com", queryResult.Results[0].Value<string>("email"));
		}
		
	}
}