// -----------------------------------------------------------------------
//  <copyright file="DamianPutSnapshot.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Database.Plugins;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bugs
{
	public class DamianPutSnapshot : RavenTest
	{
		[Fact]
		public void Cannot_modify_snapshot()
		{
			using (var documentStore = NewDocumentStore())
			{
				documentStore.Initialize();
				documentStore.DocumentDatabase.PutTriggers.Add(new PutTrigger {Database = documentStore.DocumentDatabase});
				using (IDocumentSession session = documentStore.OpenSession())
				{
					session.Store(new Doc {Id = "DocId1", Name = "Name1"});
					session.SaveChanges();
				}
			}
		}

		public class Doc
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

		public class PutTrigger : AbstractPutTrigger
		{
			public override void OnPut(string key,
			                           RavenJObject document,
			                           RavenJObject metadata,
			                           TransactionInformation transactionInformation)
			{
				using (Database.DisableAllTriggersForCurrentThread())
				{
					var revisionCopy = new RavenJObject(document);
					Database.Documents.Put("CopyOfDoc", null, revisionCopy, new RavenJObject(metadata), transactionInformation);
				}
			}
		}
	}
}