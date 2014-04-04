//-----------------------------------------------------------------------
// <copyright file="MultipleDocuments.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Transactions
{
	public class MultipleDocuments : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public MultipleDocuments()
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
            EnsureDtcIsSupported(db);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende1", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			db.Documents.Put("ayende2", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Documents.Get("ayende1", null));
			Assert.NotNull(db.Documents.Get("ayende2", null));
		}

		[Fact]
		public void CommittingWillOnlyCommitSingleTransaction()
		{
            EnsureDtcIsSupported(db);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende1", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
            db.Documents.Put("ayende2", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) });

			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Documents.Get("ayende1", null));
			Assert.True(db.Documents.Get("ayende2", null).Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));
		}

		[Fact]
		public void PutTwoDocumentsAndThenCommitReversedOrder()
		{
            EnsureDtcIsSupported(db);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende2", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			db.Documents.Put("ayende1", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Documents.Get("ayende1", null));
			Assert.NotNull(db.Documents.Get("ayende2", null));
		}

		[Fact]
		public void WhileUpdatingSeveralDocumentsCannotAccessAnyOfThem()
        {
            EnsureDtcIsSupported(db);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende1", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.True(db.Documents.Get("ayende1", null).Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));
			Assert.Null(db.Documents.Get("ayende2", null)); 
			db.Documents.Put("ayende2", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.True(db.Documents.Get("ayende1", null).Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));
			Assert.True(db.Documents.Get("ayende2", null).Metadata.Value<bool>(Constants.RavenDocumentDoesNotExists));

			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);

			Assert.NotNull(db.Documents.Get("ayende1", null));
			Assert.NotNull(db.Documents.Get("ayende2", null));
		}
	}
}
