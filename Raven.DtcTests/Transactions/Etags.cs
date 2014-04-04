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
using Raven.Tests.Common;

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
            EnsureDtcIsSupported(db);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var doc = db.Documents.Get("ayende", null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", doc.Etag, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);


			Assert.Equal("rahien", db.Documents.Get("ayende", null).ToJson()["ayende"].Value<string>());
		}

		[Fact]
		public void AfterPuttingDocInTxWillChangeEtag()
		{
            EnsureDtcIsSupported(db);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var doc = db.Documents.Get("ayende", null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", doc.Etag, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			var docInTx = db.Documents.Get("ayende", transactionInformation);
			db.Documents.Put("ayende", docInTx.Etag, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);

			Assert.NotEqual(doc.Etag, docInTx.Etag);

		}

		[Fact]
		public void AfterCommitWillNotRetainSameEtag()
		{
            EnsureDtcIsSupported(db);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
			var doc = db.Documents.Get("ayende", null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			db.Documents.Put("ayende", doc.Etag, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(), transactionInformation);
			var docInTx = db.Documents.Get("ayende", transactionInformation);
			db.PrepareTransaction(transactionInformation.Id);
			db.Commit(transactionInformation.Id);
			var docAfterTx = db.Documents.Get("ayende", null);

			Assert.NotEqual(docAfterTx.Etag, docInTx.Etag);
		}

		[Fact]
		public void WhenUsingTransactionWillFailIfDocumentEtagDoesNotMatch()
		{
            EnsureDtcIsSupported(db);
			db.Documents.Put("ayende", null, RavenJObject.Parse("{ayende:'oren'}"), new RavenJObject(), null);
            var transactionInformation = new TransactionInformation { Id = Guid.NewGuid().ToString(), Timeout = TimeSpan.FromMinutes(1) };
			Assert.Throws<ConcurrencyException>(
				() =>
				db.Documents.Put("ayende", Etag.InvalidEtag, RavenJObject.Parse("{ayende:'rahien'}"), new RavenJObject(),
					   transactionInformation));
		}
	}
}
