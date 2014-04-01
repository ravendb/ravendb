//-----------------------------------------------------------------------
// <copyright file="WriteConflicts.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Database;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Transactions
{
	public class WriteConflicts : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public WriteConflicts()
		{
			store = NewDocumentStore(requestedStorage: "esent");
			db = store.DocumentDatabase;
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void WhileDocumentIsBeingUpdatedInTransactionCannotUpdateOutsideTransaction()
		{
            EnsureDtcIsSupported(db);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.Throws<ConcurrencyException>(
				() => db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null));
		}

		[Fact]
		public void WhileDocumentIsBeingUpdatedInTransactionCannotUpdateInsideAnotherTransaction()
		{
            EnsureDtcIsSupported(db);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.Throws<ConcurrencyException>(
				() => db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), new TransactionInformation
				{
                    Id = Guid.NewGuid().ToString(),
					Timeout = TimeSpan.FromMinutes(1)
				}));
		}


		[Fact]
		public void WhileCreatingDocumentInTransactionTryingToWriteInAnotherTransactionFail()
		{
            EnsureDtcIsSupported(db);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.Throws<ConcurrencyException>(
				() => db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), new TransactionInformation
				{
                    Id = Guid.NewGuid().ToString(),
					Timeout = TimeSpan.FromMinutes(1)
				}));
		}

		[Fact]
		public void WhileCreatingDocumentInTransactionTryingToWriteOutsideTransactionFail()
		{
            EnsureDtcIsSupported(db);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.Throws<ConcurrencyException>(
				() => db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null));
		}
	}
}
