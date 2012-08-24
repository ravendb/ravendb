//-----------------------------------------------------------------------
// <copyright file="MuiltipleDocuments.cs" company="Hibernating Rhinos LTD">
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
	public class MuiltipleDocuments : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public MuiltipleDocuments()
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
		public void PutTwoDocumentsAndThenCommit()
		{
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende1", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			db.Put("ayende2", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Get("ayende1", null));
			Assert.NotNull(db.Get("ayende2", null));
		}

		[Fact]
		public void CommittingWillOnlyCommitSingleTransaction()
		{
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende1", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			db.Put("ayende2", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) });

			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Get("ayende1", null));
			Assert.True(db.Get("ayende2", null).Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));
		}

		[Fact]
		public void PutTwoDocumentsAndThenCommitReversedOrder()
		{
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende2", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			db.Put("ayende1", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Get("ayende1", null));
			Assert.NotNull(db.Get("ayende2", null));
		}

		[Fact]
		public void WhileUpdatingSeveralDocumentsCannotAccessAnyOfThem()
		{
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende1", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.True(db.Get("ayende1", null).Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));
			Assert.Null(db.Get("ayende2", null)); 
			db.Put("ayende2", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.True(db.Get("ayende1", null).Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));
			Assert.True(db.Get("ayende2", null).Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));
			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Get("ayende1", null));
			Assert.NotNull(db.Get("ayende2", null));
		}
	}
}
