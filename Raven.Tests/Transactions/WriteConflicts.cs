using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Exceptions;
using Raven.Http;
using Raven.Http.Exceptions;
using Raven.Tests.Storage;
using Xunit;

namespace Raven.Tests.Transactions
{
    public class WriteConflicts : AbstractDocumentStorageTest
    {
        private readonly DocumentDatabase db;

        public WriteConflicts()
        {
			db = new DocumentDatabase(new RavenConfiguration { DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true });
        }

        public override void Dispose()
        {
            db.Dispose();
            base.Dispose();
        }

        [Fact]
        public void WhileDocumentIsBeingUpdatedInTransactionCannotUpdateOutsideTransaction()
        {
            db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);
            Assert.Throws<ConcurrencyException>(
                () => db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null));
        }

        [Fact]
        public void WhileDocumentIsBeingUpdatedInTransactionCannotUpdateInsideAnotherTransaction()
        {
            db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);
            Assert.Throws<ConcurrencyException>(
                () => db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), new TransactionInformation
                {
                    Id = Guid.NewGuid(),
                    Timeout = TimeSpan.FromMinutes(1)
                }));
        }


        [Fact]
        public void WhileCreatingDocumentInTransactionTryingToWriteInAnotherTransactionFail()
        {
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);
            Assert.Throws<ConcurrencyException>(
                () => db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), new TransactionInformation
                {
                    Id = Guid.NewGuid(),
                    Timeout = TimeSpan.FromMinutes(1)
                }));
        }

        [Fact]
        public void WhileCreatingDocumentInTransactionTryingToWriteOutsideTransactionFail()
        {
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
            db.Put("ayende", null, JObject.Parse("{ayende:'rahien'}"), new JObject(), transactionInformation);
            Assert.Throws<ConcurrencyException>(
                () => db.Put("ayende", null, JObject.Parse("{ayende:'oren'}"), new JObject(), null));
        }
    }
}
