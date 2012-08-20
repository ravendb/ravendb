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
using Raven.Database.Config;
using Raven.Tests.Storage;
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
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Delete("ayende", null, transactionInformation);
			Assert.NotNull(db.Get("ayende", null));
		}

		[Fact]
		public void DeletingDocumentInTransactionInNotFoundInSameTransactionBeforeCommit()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Delete("ayende", null, transactionInformation);
			Assert.Null(db.Get("ayende", transactionInformation));
	   
		}

		[Fact]
		public void DeletingDocumentAndThenAddingDocumentInSameTransactionCanWork()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Delete("ayende", null, transactionInformation);
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			db.Commit(transactionInformation.Id);

			Assert.Equal("rahien", db.Get("ayende", null).ToJson()["ayende"].Value<string>());
		
		}

		[Fact]
		public void DeletingDocumentInTransactionInRemovedAfterCommit()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var transactionInformation = new TransactionInformation { Id = Guid.NewGuid(), Timeout = TimeSpan.FromMinutes(1) };
			db.Delete("ayende", null, transactionInformation);
			db.Commit(transactionInformation.Id);

			Assert.Null(db.Get("ayende", null));
		
		}
	}
}
