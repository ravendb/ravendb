using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Exceptions;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Transactions
{
    public class Etags : AbstractDocumentStorageTest
    {
        private readonly DocumentDatabase db;

        public Etags()
        {
			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true });
        }

        public override void Dispose()
        {
            db.Dispose();
            base.Dispose();
        }

        [Fact]
        public void WhenUsingTransactionWillWorkIfDocumentEtagMatch()
        {
            db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null);
            var doc = db.Get("ayende", null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", doc.Etag, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);
            db.Commit(transactionInformation.Id);


            Assert.Equal("rahien", db.Get("ayende", null).ToJson()["ayende"].Value<string>());
        }

        [Fact]
        public void AfterPuttingDocInTxWillChangeEtag()
        {
            db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null);
            var doc = db.Get("ayende", null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", doc.Etag, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);
            var docInTx = db.Get("ayende", transactionInformation);
            db.Put("ayende", docInTx.Etag, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);

            Assert.NotEqual(doc.Etag, docInTx.Etag);

        }

        [Fact]
        public void AfterCommitWillNotRetainSameEtag()
        {
            db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null);
            var doc = db.Get("ayende", null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", doc.Etag, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);
            var docInTx = db.Get("ayende", transactionInformation);
            db.Commit(transactionInformation.Id);
            var docAfterTx = db.Get("ayende", null);

            Assert.NotEqual(docAfterTx.Etag, docInTx.Etag);
        }

        [Fact]
        public void WhenUsingTransactionWillFailIfDocumentEtagDoesNotMatch()
        {
            db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            Assert.Throws<ConcurrencyException>(
                () =>
                db.Put("ayende", Guid.NewGuid(), JObject.Parse("{ayende:'rahien'}"), new JObject(),
                       transactionInformation));
        }
    }
}
