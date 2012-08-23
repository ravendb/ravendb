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
using Raven.Database.Config;
using Raven.Tests.Storage;
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
			var transactionInformation = new TransactionInformation{Id = Guid.NewGuid(),Timeout = TimeSpan.FromMinutes(1)};
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Get("ayende", null));
		}

		[Fact]
		public void PutNewDocInTxAndThenGetItBeforeCommitReturnsNull()
		{
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			Assert.True(db.Get("ayende", null).Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));
		}


		[Fact]
		public void PutNewDocInTxAndThenGetItBeforeCommitInSameTransactionReturnsNonNull()
		{
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			Assert.NotNull(db.Get("ayende", transactionInformation));
		}

		[Fact]
		public void UpdateDocInTxCommitAndThenGetIt()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Get("ayende", null));
			Assert.Equal("rahien", db.Get("ayende", null).ToJson()["ayende"].Value<string>());
		}


		[Fact]
		public void UpdateDocInTxAndThenGetItBeforeCommit()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			Assert.NotNull(db.Get("ayende", null));
			Assert.Equal("oren", db.Get("ayende", null).ToJson()["ayende"].Value<string>());
		}

		[Fact]
		public void UpdateDocInTxAndThenGetItBeforeCommitInSameTx()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			Assert.NotNull(db.Get("ayende", transactionInformation));
			Assert.Equal("rahien", db.Get("ayende", transactionInformation).ToJson()["ayende"].Value<string>());
		}


		[Fact]
		public void SeveralUdpatesInTheSameTransaction()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien1'}"), new RavenJObject(), transactionInformation);
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien2'}"), new RavenJObject(), transactionInformation);
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien3'}"), new RavenJObject(), transactionInformation);

			Assert.Equal("oren", db.Get("ayende", null).ToJson()["ayende"].Value<string>());
			Assert.Equal("rahien3", db.Get("ayende", transactionInformation).ToJson()["ayende"].Value<string>());
			db.Commit(transactionInformation.Id);

			Assert.Equal("rahien3", db.Get("ayende", null).ToJson()["ayende"].Value<string>());
		}

	}
}
