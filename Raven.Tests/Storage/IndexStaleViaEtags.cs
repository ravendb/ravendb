//-----------------------------------------------------------------------
// <copyright file="IndexStaleViaEtags.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System.Threading;
using Raven.Client.Embedded;
using Raven.Json.Linq;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Config;
using Xunit;

namespace Raven.Tests.Storage
{
	public class IndexStaleViaEtags : RavenTest
	{
		private readonly EmbeddableDocumentStore store;
		private readonly DocumentDatabase db;

		public IndexStaleViaEtags()
		{
			store = NewDocumentStore();
			db = store.DocumentDatabase;
			db.PutIndex(new RavenDocumentsByEntityName().IndexName, new RavenDocumentsByEntityName().CreateIndexDefinition());
		}

		public override void Dispose()
		{
			store.Dispose();
			base.Dispose();
		}

		[Fact]
		public void CanTellThatIndexIsStale()
		{
			db.TransactionalStorage.Batch(accessor => Assert.False(accessor.Staleness.IsIndexStale("Raven/DocumentsByEntityName", null, null)));

			db.Put("ayende", null, new RavenJObject(), new RavenJObject(), null);

			db.TransactionalStorage.Batch(accessor => Assert.True(accessor.Staleness.IsIndexStale("Raven/DocumentsByEntityName", null, null)));
		}

		[Fact]
		public void CanIndexDocuments()
		{
			db.TransactionalStorage.Batch(accessor => Assert.False(accessor.Staleness.IsIndexStale("Raven/DocumentsByEntityName", null, null)));

			db.Put("ayende", null, new RavenJObject(), new RavenJObject(), null);

			bool indexed = false;
			for (int i = 0; i < 500; i++)
			{
				db.TransactionalStorage.Batch(accessor => indexed = (accessor.Staleness.IsIndexStale("Raven/DocumentsByEntityName", null, null)));
				if (indexed == false)
					break;
				Thread.Sleep(50);
			}

			Assert.False(indexed);
		}
	}
}
