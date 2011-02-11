	using Newtonsoft.Json.Linq;
using Raven.Client;
using Raven.Database.Data;
using Raven.Database.Json;
using Xunit;

namespace Raven.Tests.Patching
{
	public class MetadataPatching : LocalClientTest
	{
		[Fact]
		public void ChangeRavenEntityName()
		{
			using (var store = NewDocumentStore())
			{
				store.DocumentDatabase.Put("foos/1", null, JObject.Parse("{'Something':'something'}"),
					JObject.Parse("{'Raven-Entity-Name': 'Foos'}"), null);
				WaitForIndexing(store);
				store.DatabaseCommands.UpdateByIndex(RavenExtensions.RavenDocumentByEntityName,
					new IndexQuery(), new[]
					{
						new PatchRequest
						{
							Type = PatchCommandType.Modify,
							Name = "@metadata",
							Nested = new []
							{
								new PatchRequest
								{
									Type = PatchCommandType.Set,
									Name = "Raven-Entity-Name",
									Value = new JValue("Bars")
								}
							}
						}
							
					}, false);
				var jsonDocument = store.DocumentDatabase.Get("foos/1", null);
				Assert.Equal("Bars", jsonDocument.Metadata.Value<string>("Raven-Entity-Name"));
			}
		}
	}
}