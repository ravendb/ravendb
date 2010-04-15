using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Transactions
{
    public class MuiltipleDocuments : AbstractDocumentStorageTest
    {
        private readonly DocumentDatabase db;

		public MuiltipleDocuments()
		{
			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = "raven.db.test.esent"});
		}

        public override void Dispose()
        {
            db.Dispose();
            base.Dispose();
        }

        [Fact]
        public void PutTwoDocumentsAndThenCommit()
        {
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende1", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);
            db.Put("ayende2", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);

            db.Commit(transactionInformation.Id);

            Assert.NotNull(db.Get("ayende1", null));
            Assert.NotNull(db.Get("ayende2", null));
        }

        [Fact]
        public void WhileUpdatingSeveralDocumentsCannotAccessAnyOfThem()
        {
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende1", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);
            Assert.Null(db.Get("ayende1", null));
            Assert.Null(db.Get("ayende2", null)); 
            db.Put("ayende2", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);
            Assert.Null(db.Get("ayende1", null));
            Assert.Null(db.Get("ayende2", null));
            db.Commit(transactionInformation.Id);

            Assert.NotNull(db.Get("ayende1", null));
            Assert.NotNull(db.Get("ayende2", null));
        }
    }
}