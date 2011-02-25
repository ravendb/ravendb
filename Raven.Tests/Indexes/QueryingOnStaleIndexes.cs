//-----------------------------------------------------------------------
// <copyright file="QueryingOnStaleIndexes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading;
using Newtonsoft.Json.Linq;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Data;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Indexes
{
    public class QueryingOnStaleIndexes: AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public QueryingOnStaleIndexes()
		{
			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true });
			db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
		

		}

		#region IDisposable Members

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		#endregion

        [Fact]
        public void WillGetStaleResultWhenThereArePendingTasks()
        {
            db.Put("a", null, new JObject(), new JObject(), null);

            Assert.True(db.Query("Raven/DocumentsByEntityName", new IndexQuery
            {
                PageSize = 2,
                Start = 0,
            }).IsStale);
        }

        [Fact]
        public void WillGetNonStaleResultWhenAskingWithCutoffDate()
        {
			db.SpinBackgroundWorkers();
            db.Put("a", null, new JObject(), new JObject(), null);

        	for (int i = 0; i < 50; i++)
        	{
        		var queryResult = db.Query("Raven/DocumentsByEntityName", new IndexQuery
        		{
        			PageSize = 2,
        			Start = 0,
        		});
        		if (queryResult.IsStale == false)
					break;
				Thread.Sleep(100);
        	}

			Assert.False(db.Query("Raven/DocumentsByEntityName", new IndexQuery
			{
				PageSize = 2,
				Start = 0,
			}).IsStale);

			db.StopBackgroundWokers();

			db.Put("a", null, new JObject(), new JObject(), null);


			Assert.True(db.Query("Raven/DocumentsByEntityName", new IndexQuery
			{
				PageSize = 2,
				Start = 0,
			}).IsStale);

        	Assert.False(db.Query("Raven/DocumentsByEntityName", new IndexQuery
            {
                PageSize = 2,
                Start = 0,
				Cutoff = DateTime.UtcNow.AddHours(-1)
            }).IsStale);
        }
        
    }
}
