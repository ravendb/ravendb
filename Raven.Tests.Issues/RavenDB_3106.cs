// -----------------------------------------------------------------------
//  <copyright file="UnknownIssue.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.IO;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3106 : ReplicationBase
	{
		private class Item
		{
			public string Id { get; set; }
		}

		[Fact]
		public void ReplicationShouldReplicateAllDocumentTombstones()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				new RavenDocumentsByEntityName().Execute(store1);
				new RavenDocumentsByEntityName().Execute(store2);

				TellFirstInstanceToReplicateToSecondInstance();
				string lastId = null;

				using (var session = store1.OpenSession())
				{
					for (int i = 0; i < 2048; i++)
					{
						var item = new Item();
						session.Store(item);
						lastId = item.Id;
					}

					session.SaveChanges();
				}

				WaitForIndexing(store1);
				WaitForReplication(store2, lastId);

				RemoveReplication(store1.DatabaseCommands);

				store1
					.DatabaseCommands
					.DeleteByIndex("Raven/DocumentsByEntityName", new IndexQuery { Query = "Tag:Items" })
					.WaitForCompletion();

				WaitForIndexing(store1);

				using (var session = store1.OpenSession())
				{
					var item = new Item();
					session.Store(item);
					lastId = item.Id;

					session.SaveChanges();
				}

				TellFirstInstanceToReplicateToSecondInstance();
				WaitForReplication(store2, lastId);
				WaitForIndexing(store2);

				using (var session = store2.OpenSession())
				{
					var items = session
						.Query<Item>()
						.Take(1024)
						.ToList();

					Assert.Equal(1, items.Count);
					Assert.Equal(lastId, items.First().Id);
				}
			}
		}

		[Fact]
		public void ReplicationShouldReplicateAllAttachmentTombstones()
		{
			using (var store1 = CreateStore())
			using (var store2 = CreateStore())
			{
				TellFirstInstanceToReplicateToSecondInstance();
				string lastId = null;

				for (int i = 0; i < 200; i++)
				{
					var id = "static/" + i;
					store1.DatabaseCommands.PutAttachment(id, null, new MemoryStream(), new RavenJObject());
					lastId = id;
				}

				WaitForIndexing(store1);
				WaitForAttachment(store2, lastId);

				RemoveReplication(store1.DatabaseCommands);

				for (int i = 0; i < 200; i++)
				{
					var id = "static/" + i;
					store1.DatabaseCommands.DeleteAttachment(id, null);
				}

				store1.DatabaseCommands.PutAttachment("static/9999", null, new MemoryStream(), new RavenJObject());
				lastId = "static/9999";

				TellFirstInstanceToReplicateToSecondInstance();
				WaitForAttachment(store2, lastId);

				var attachments = store2
					.DatabaseCommands
					.GetAttachmentHeadersStartingWith("static/", 0, int.MaxValue)
					.ToList();

				Assert.Equal(1, attachments.Count);
				Assert.Equal(lastId, attachments.First().Key);
			}
		}
	}
}