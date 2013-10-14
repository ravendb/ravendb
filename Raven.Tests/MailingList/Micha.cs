using System.Linq;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Tests.MailingList
{
	public class Micha : RavenTest
	{
		public class Entity
		{
			public string Label { get; set; }	
		}

		public class EntityEntityIdPatch : AbstractIndexCreationTask<Entity>
		{
			public EntityEntityIdPatch()
			{
				Map = docs => from doc in docs
							  select new { doc.Label };
			}
		}

		[Fact]
		public void CanDeleteIndex()
		{
			using(var store = NewDocumentStore())
			{
				new EntityEntityIdPatch().Execute(store);

				WaitForIndexing(store);

				store.DatabaseCommands.UpdateByIndex("EntityEntityIdPatch",
				                                     new IndexQuery(),
				                                     new[]
				                                     {
				                                     	new PatchRequest()
				                                     	{
				                                     		Type = PatchCommandType.Rename,
				                                     		Name = "EntityType",
				                                     		Value = new RavenJValue("EntityTypeId")
				                                     	}
				                                     }, false);
			    var id = store.DocumentDatabase.IndexDefinitionStorage.GetIndexDefinition("EntityEntityIdPatch").IndexId;
				store.DatabaseCommands.DeleteIndex("EntityEntityIdPatch");

				Assert.False(store.DocumentDatabase.Statistics.Indexes.Any(x => x.Id == id));
			}
		}
	}
}