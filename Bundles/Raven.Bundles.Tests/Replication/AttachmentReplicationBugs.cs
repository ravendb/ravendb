extern alias database;

using System.IO;
using System.Threading;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Bundles.Tests.Replication
{
	public class AttachmentReplicationBugs : ReplicationBase
	{
		protected override void ConfigureServer(database::Raven.Database.Config.RavenConfiguration serverConfiguration)
		{
			serverConfiguration.RunInMemory = false;
			serverConfiguration.DefaultStorageTypeName = "esent";
		}

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

	}
}