//-----------------------------------------------------------------------
// <copyright file="Deletes.cs" company="Hibernating Rhinos LTD">
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
	public class Deletes : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public Deletes()
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
		public void DeletingDocumentInTransactionInNotVisibleBeforeCommit()
		{
            EnsureDtcIsSupported(db);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Delete("ayende", null, transactionInformation);
			Assert.NotNull(db.Documents.Get("ayende", null));
		}

		[Fact]
		public void DeletingDocumentInTransactionInNotFoundInSameTransactionBeforeCommit()
		{
            EnsureDtcIsSupported(db);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Delete("ayende", null, transactionInformation);
			Assert.Null(db.Documents.Get("ayende", transactionInformation));
	   
		}

		[Fact]
		public void DeletingDocumentAndThenAddingDocumentInSameTransactionCanWork()
		{
            EnsureDtcIsSupported(db);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Delete("ayende", null, transactionInformation);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);

			Assert.Equal("rahien", db.Documents.Get("ayende", null).ToJson()["ayende"].Value<string>());
		
		}

		[Fact]
		public void DeletingDocumentInTransactionInRemovedAfterCommit()
		{
            EnsureDtcIsSupported(db);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Delete("ayende", null, transactionInformation);
			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);

			Assert.Null(db.Documents.Get("ayende", null));
		
		}
	}
}
