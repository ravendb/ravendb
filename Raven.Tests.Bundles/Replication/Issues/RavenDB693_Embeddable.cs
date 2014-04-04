using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Bundles.Replication.Issues
{
	public class RavenDB693_Embeddable : ReplicationBase
	{
		[Fact]
		public void CanHandleConflictsOnClient_Embeddable()
		{
			using (var store1 = CreateEmbeddableStore())
			{
				store1.DatabaseCommands.Put("ayende", null, new RavenJObject
				{
					{"Name", "Ayende"}
				}, new RavenJObject());

				store1.DatabaseCommands.Put("marker", null, new RavenJObject(), new RavenJObject());

				using (var store2 = CreateEmbeddableStore())
				{
					store2.DatabaseCommands.Put("ayende", null, new RavenJObject
					{
						{"Name", "Rahien"}
					}, new RavenJObject());

					TellFirstInstanceToReplicateToSecondInstance();

					WaitForReplication(store2, "marker");

					store2.RegisterListener(new ClientSideConflictResolution());

					var jsonDocument = store2.DatabaseCommands.Get("ayende");

					Assert.Equal("Ayende Rahien", jsonDocument.DataAsJson.Value<string>("Name"));
				}
			}
		}

		[Fact]
		public void CanHandleConflictsOnClient_Embeddable_Async()
		{
			using (var store1 = CreateEmbeddableStore())
			{
				store1.DatabaseCommands.Put("ayende", null, new RavenJObject
				{
					{"Name", "Ayende"}
				}, new RavenJObject());

				store1.DatabaseCommands.Put("marker", null, new RavenJObject(), new RavenJObject());

				using (var store2 = CreateEmbeddableStore())
				{
					store2.DatabaseCommands.Put("ayende", null, new RavenJObject
					{
						{"Name", "Rahien"}
					}, new RavenJObject());

					TellFirstInstanceToReplicateToSecondInstance();

					WaitForReplication(store2, "marker");

					store2.RegisterListener(new ClientSideConflictResolution());

					var jsonDocument = store2.AsyncDatabaseCommands.GetAsync("ayende").Result;

					Assert.Equal("Ayende Rahien", jsonDocument.DataAsJson.Value<string>("Name"));
				}
			}
		}
	}
}