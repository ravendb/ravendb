using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
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
			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true});
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
            db.Put("a", null, new JObject(), new JObject(), null);

            Assert.False(db.Query("Raven/DocumentsByEntityName", new IndexQuery
            {
                PageSize = 2,
                Start = 0,
                Cutoff = DateTime.Now.AddHours(-1)
            }).IsStale);
        }
        
    }
}