// -----------------------------------------------------------------------
//  <copyright file="DtcAndTouchDocument.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class DtcAndTouchDocument : RavenTest
	{
		[Fact]
		public void ShouldWork()
		{
			using (EmbeddableDocumentStore store = NewDocumentStore())
			{
				PutResult putResult = store.DocumentDatabase.Put("test", null, new RavenJObject(), new RavenJObject(), null);

				var transactionInformation = new TransactionInformation
				{
					Id = "tx",
					Timeout = TimeSpan.FromDays(1)
				};
				Raven.Abstractions.Data.Etag etag;
				
				store.DocumentDatabase.Put("test", putResult.ETag, new RavenJObject(), new RavenJObject(), transactionInformation);

				store.DocumentDatabase.TransactionalStorage.Batch(accessor =>
					accessor.Documents.TouchDocument("test", out etag, out etag));
				
				store.DocumentDatabase.PrepareTransaction("tx");
				store.DocumentDatabase.Commit("tx");
			}
		}
	}
}