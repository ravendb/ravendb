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
using Raven.Database.Config;
using Raven.Tests.Storage;
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
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.Throws<ConcurrencyException>(
				() => db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null));
		}

		[Fact]
		public void WhileDocumentIsBeingUpdatedInTransactionCannotUpdateInsideAnotherTransaction()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.Throws<ConcurrencyException>(
				() => db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), new TransactionInformation
				{
					Id = Guid.NewGuid(),
					Timeout = TimeSpan.FromMinutes(1)
				}));
		}


		[Fact]
		public void WhileCreatingDocumentInTransactionTryingToWriteInAnotherTransactionFail()
		{
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.Throws<ConcurrencyException>(
				() => db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), new TransactionInformation
				{
					Id = Guid.NewGuid(),
					Timeout = TimeSpan.FromMinutes(1)
				}));
		}

		[Fact]
		public void WhileCreatingDocumentInTransactionTryingToWriteOutsideTransactionFail()
		{
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			Assert.Throws<ConcurrencyException>(
				() => db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null));
		}
	}
}
