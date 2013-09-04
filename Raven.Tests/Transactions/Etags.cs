//-----------------------------------------------------------------------
// <copyright file="Etags.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Database;
using Xunit;

namespace Raven.Tests.Transactions
{
	public class Etags : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public Etags()
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
		public void WhenUsingTransactionWillWorkIfDocumentEtagMatch()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var doc = db.Get("ayende", null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", doc.Etag, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);


			Assert.Equal("rahien", db.Get("ayende", null).ToJson()["ayende"].Value<string>());
		}

		[Fact]
		public void AfterPuttingDocInTxWillChangeEtag()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var doc = db.Get("ayende", null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", doc.Etag, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			var docInTx = db.Get("ayende", transactionInformation);
			db.Put("ayende", docInTx.Etag, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			Assert.NotEqual(doc.Etag, docInTx.Etag);

		}

		[Fact]
		public void AfterCommitWillNotRetainSameEtag()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var doc = db.Get("ayende", null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Put("ayende", doc.Etag, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			var docInTx = db.Get("ayende", transactionInformation);
			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);
			var docAfterTx = db.Get("ayende", null);

			Assert.NotEqual(docAfterTx.Etag, docInTx.Etag);
		}

		[Fact]
		public void WhenUsingTransactionWillFailIfDocumentEtagDoesNotMatch()
		{
			db.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			Assert.Throws<ConcurrencyException>(
				() =>
				db.Put("ayende", Etag.InvalidEtag, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(),
					   transactionInformation));
		}
	}
}
