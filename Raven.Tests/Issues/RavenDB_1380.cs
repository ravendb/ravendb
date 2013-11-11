// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1380.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using Raven.Abstractions.Data;
	using Raven.Json.Linq;

	using Xunit;

	public class RavenDB_1380 : RavenTest
	{
		[Fact]
		public void PatchingShouldBeDisabledForDocumentsWithDeleteMarkerWhenReplicationIsTurnedOn()
		{
			using (var store = NewDocumentStore())
			{
				store.DatabaseCommands.Put(
					"docs/1", null, new RavenJObject(), new RavenJObject
					                                     {
						                                     { Constants.RavenDeleteMarker, "true" }
					                                     });

				store.DatabaseCommands.Put(
					"docs/2", null, new RavenJObject(), new RavenJObject());

				var result = store.DocumentDatabase.ApplyPatch("docs/1", null, new ScriptedPatchRequest { Script = "" }, null);
				Assert.Equal(PatchResult.DocumentDoesNotExists, result.Item1.PatchResult);

				result = store.DocumentDatabase.ApplyPatch("docs/2", null, new ScriptedPatchRequest { Script = @"this[""Test""] = 999;" }, null);
				Assert.Equal(PatchResult.Patched, result.Item1.PatchResult);

				Assert.Equal(999, store.DocumentDatabase.Get("docs/2", null).DataAsJson.Value<int>("Test"));
			}
		}

		protected override void ModifyConfiguration(Database.Config.InMemoryRavenConfiguration configuration)
		{
			configuration.Settings["Raven/ActiveBundles"] = "replication";
		}
	}
}