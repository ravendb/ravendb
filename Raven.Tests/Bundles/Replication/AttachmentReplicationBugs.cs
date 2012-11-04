using System.IO;
using System.Threading;
using Raven.Client.Exceptions;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.Bundles.Replication
{
	public class AttachmentReplicationBugs : ReplicationBase
	{
		[Fact]
		public void Can_replicate_documents_between_two_external_instances()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			var databaseCommands = store1.DatabaseCommands;
			const int documentCount = 20;
			for (int i = 0; i < documentCount; i++)
			{
				databaseCommands.PutAttachment(i.ToString(), null, new MemoryStream(new[] { (byte)i }), new RavenJObject());
			}

			bool foundAll = false;
			for (int i = 0; i < RetriesCount; i++)
			{
				var countFound = 0;
				for (int j = 0; j < documentCount; j++)
				{
					var attachment = store2.DatabaseCommands.GetAttachment(j.ToString());
					if (attachment == null)
						break;
					countFound++;
				}
				foundAll = countFound == documentCount;
				if (foundAll)
					break;
				Thread.Sleep(100);
			}
			Assert.True(foundAll);
		}


		[Fact]
		public void Can_resolve_conflict_with_delete()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();


			store1.DatabaseCommands.PutAttachment("static", null, new MemoryStream(new[] { (byte)1 }), new RavenJObject());
			store2.DatabaseCommands.PutAttachment("static", null, new MemoryStream(new[] { (byte)1 }), new RavenJObject());

			TellFirstInstanceToReplicateToSecondInstance();

			var conflictException = Assert.Throws<ConflictException>(() =>
			{
				for (int i = 0; i < RetriesCount; i++)
				{
					store2.DatabaseCommands.GetAttachment("static");
					Thread.Sleep(100);
				}
			});

			store2.DatabaseCommands.DeleteAttachment("static", null);


			foreach (var conflictedVersionId in conflictException.ConflictedVersionIds)
			{
				Assert.Null(store2.DatabaseCommands.GetAttachment(conflictedVersionId));
			}
		}
	}
}