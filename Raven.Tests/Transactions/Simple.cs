//-----------------------------------------------------------------------
// <copyright file="Simple.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Database;
using Xunit;

namespace Raven.Tests.Transactions
{
	public class Simple : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public Simple()
		{
			store = NewDocumentStore();
			db = store.DocumentDatabase;
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void PutNewDocInTxCommitAndThenGetIt()
		{
            if (db.TransactionalStorage.SupportsDtc == false)
                return;
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Documents.Get("ayende", null));
		}

		[Fact]
		public void PutNewDocInTxAndThenGetItBeforeCommitReturnsNull()
		{
            if (db.TransactionalStorage.SupportsDtc == false)
                return;
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			Assert.True(db.Documents.Get("ayende", null).Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));
		}


		[Fact]
		public void PutNewDocInTxAndThenGetItBeforeCommitInSameTransactionReturnsNonNull()
		{
            if (db.TransactionalStorage.SupportsDtc == false)
                return;
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			Assert.NotNull(db.Documents.Get("ayende", transactionInformation));
		}

		[Fact]
		public void UpdateDocInTxCommitAndThenGetIt()
		{
            if (db.TransactionalStorage.SupportsDtc == false)
                return;
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Documents.Get("ayende", null));
			Assert.Equal("rahien", db.Documents.Get("ayende", null).ToJson()["ayende"].Value<string>());
		}


		[Fact]
		public void UpdateDocInTxAndThenGetItBeforeCommit()
		{
            if (db.TransactionalStorage.SupportsDtc == false)
                return;
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			Assert.NotNull(db.Documents.Get("ayende", null));
			Assert.Equal("oren", db.Documents.Get("ayende", null).ToJson()["ayende"].Value<string>());
		}

		[Fact]
		public void UpdateDocInTxAndThenGetItBeforeCommitInSameTx()
		{
            if (db.TransactionalStorage.SupportsDtc == false)
                return;
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			Assert.NotNull(db.Documents.Get("ayende", transactionInformation));
			Assert.Equal("rahien", db.Documents.Get("ayende", transactionInformation).ToJson()["ayende"].Value<string>());
		}


		[Fact]
		public void SeveralUpdatesInTheSameTransaction()
		{
            if (db.TransactionalStorage.SupportsDtc == false)
                return;
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien1'}"), new RavenJObject(), transactionInformation);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien2'}"), new RavenJObject(), transactionInformation);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien3'}"), new RavenJObject(), transactionInformation);

			Assert.Equal("oren", db.Documents.Get("ayende", null).ToJson()["ayende"].Value<string>());
			Assert.Equal("rahien3", db.Documents.Get("ayende", transactionInformation).ToJson()["ayende"].Value<string>());

			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);

			Assert.Equal("rahien3", db.Documents.Get("ayende", null).ToJson()["ayende"].Value<string>());
		}

	}
}
