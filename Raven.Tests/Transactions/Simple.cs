using System;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Transactions
{
    public class Simple : AbstractDocumentStorageTest
    {
        private readonly DocumentDatabase db;

		public Simple()
		{
			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = "raven.db.test.esent"});
		}

        public override void Dispose()
        {
            db.Dispose();
            base.Dispose();
        }

        [Fact]
        public void PutNewDocInTxCommitAndThenGetIt()
        {
            var transactionInformation = new TransactionInformation{Id = Guid.NewGuid(),Timeout = TimeSpan.FromMinutes(1)};
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);

            db.Commit(transactionInformation.Id);

            Assert.NotNull(db.Get("ayende", null));
        }

        [Fact]
        public void PutNewDocInTxAndThenGetItBeforeCommitReturnsNull()
        {
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);

            Assert.Null(db.Get("ayende", null));
        }


        [Fact]
        public void PutNewDocInTxAndThenGetItBeforeCommitInSameTransactionReturnsNonNull()
        {
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);

            Assert.NotNull(db.Get("ayende", transactionInformation));
        }

        [Fact]
        public void UpdateDocInTxCommitAndThenGetIt()
        {
            db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);

            db.Commit(transactionInformation.Id);

            Assert.NotNull(db.Get("ayende", null));
            Assert.Equal("rahien", db.Get("ayende", null).ToJson()["ayende"].Value<string>());
        }


        [Fact]
        public void UpdateDocInTxAndThenGetItBeforeCommit()
        {
            db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);

            Assert.NotNull(db.Get("ayende", null));
            Assert.Equal("oren", db.Get("ayende", null).ToJson()["ayende"].Value<string>());
        }

        [Fact]
        public void UpdateDocInTxAndThenGetItBeforeCommitInSameTx()
        {
            db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);

            Assert.NotNull(db.Get("ayende", transactionInformation));
            Assert.Equal("rahien", db.Get("ayende", transactionInformation).ToJson()["ayende"].Value<string>());
        }


        [Fact]
        public void SeveralUdpatesInTheSameTransaction()
        {
            db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien1'}"), new JObject(), transactionInformation);
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien2'}"), new JObject(), transactionInformation);
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien3'}"), new JObject(), transactionInformation);

            Assert.Equal("oren", db.Get("ayende", null).ToJson()["ayende"].Value<string>());
            Assert.Equal("rahien3", db.Get("ayende", transactionInformation).ToJson()["ayende"].Value<string>());
            db.Commit(transactionInformation.Id);

            Assert.Equal("rahien3", db.Get("ayende", null).ToJson()["ayende"].Value<string>());
        }

    }
}