//-----------------------------------------------------------------------
// <copyright file="QueryingOnDefaultIndex.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Newtonsoft.Json.Linq;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
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
			db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
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
			db.Put("users/ayende", null, JObject.Parse("{'email':'ayende@ayende.com'}"),
			       JObject.Parse("{'Raven-Entity-Name': 'Users'}"), null);

			QueryResult queryResult;
			do
			{
				queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
				{
					Query = "Tag:[[Users]]",
					PageSize = 10
				});
			} while (queryResult.IsStale);

			Assert.Equal("ayende@ayende.com", queryResult.Results[0].Value<string>("email"));
		}


        [Fact]
        public void CanPageOverDefaultIndex()
        {
            db.Put("users/ayende", null, JObject.Parse("{'email':'ayende@ayende.com'}"),
                   JObject.Parse("{'Raven-Entity-Name': 'Users'}"), null);
            db.Put("users/rob", null, JObject.Parse("{'email':'robashton@codeofrob.com'}"),
                   JObject.Parse("{'Raven-Entity-Name': 'Users'}"), null);
            db.Put("users/joe", null, JObject.Parse("{'email':'joe@bloggs.com'}"),
                   JObject.Parse("{'Raven-Entity-Name': 'Users'}"), null);

            QueryResult queryResultPageOne;
            QueryResult queryResultPageTwo;
            QueryResult queryResultPageThree;
            do
            {
                queryResultPageOne = db.Query("Raven/DocumentsByEntityName", new IndexQuery
                {
                    Query = "Tag:[[Users]]",
                    Start = 0,
					PageSize = 2,
					SortedFields = new [] { new SortedField("__document_id"), }
                });
            } while (queryResultPageOne.IsStale);
            do
            {
                queryResultPageTwo = db.Query("Raven/DocumentsByEntityName", new IndexQuery
                {
                    Query = "Tag:[[Users]]",
                    Start = 1,
					PageSize = 2,
					SortedFields = new [] { new SortedField("__document_id"), }
                });
            } while (queryResultPageTwo.IsStale);

            do
            {
                queryResultPageThree = db.Query("Raven/DocumentsByEntityName", new IndexQuery
                {
                    Query = "Tag:[[Users]]",
                    Start = 2,
                    PageSize = 2,
					SortedFields = new []{new SortedField("__document_id"), }
                });
            } while (queryResultPageThree.IsStale);

            // Page one
			Assert.Equal(2, queryResultPageOne.Results.Count);
			Assert.Equal("ayende@ayende.com", queryResultPageOne.Results[0].Value<string>("email"));
			Assert.Equal("joe@bloggs.com", queryResultPageOne.Results[1].Value<string>("email"));

            // Page two
			Assert.Equal(2, queryResultPageTwo.Results.Count);
			Assert.Equal("joe@bloggs.com", queryResultPageTwo.Results[0].Value<string>("email"));
			Assert.Equal("robashton@codeofrob.com", queryResultPageTwo.Results[1].Value<string>("email"));

            // Page three
			Assert.Equal(1, queryResultPageThree.Results.Count);
			Assert.Equal("robashton@codeofrob.com", queryResultPageThree.Results[0].Value<string>("email"));
        }
		
	}
}
