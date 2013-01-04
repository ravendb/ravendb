using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Data;
using Raven.Database.Indexing;
using Raven.Database.Json;
using Raven.Json.Linq;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bugs
{
	public class PatchingEntities : RavenTest
	{

		[Fact]
		public void Replacing_Value()
		{
			const string oldTagName = "old";
			using(var store = NewDocumentStore())
			{

				store.DatabaseCommands.PutIndex("MyIndex", new IndexDefinition
				{
					Map = "from doc in docs from note in doc.Comment.Notes select new { note}"
				});

				store.DatabaseCommands.Put("items/1", null, RavenJObject.FromObject(new
				{
					Comment = new
					{
						Notes = new[] {"old", "item"}
					}
				}), new RavenJObject());

				store.OpenSession().Advanced.LuceneQuery<object>("MyIndex").WaitForNonStaleResults().ToList();

				store.DatabaseCommands.UpdateByIndex("MyIndex",
				   new IndexQuery
				   {
					   Query = "note:" + oldTagName
				   },
				   new[]
				   {
					   new PatchRequest
					   {
						   Name = "Comment",
						   Type = PatchCommandType.Modify,
						   AllPositions = true,
						   Nested = new[]
						   {
							   new PatchRequest
							   {
								   Type = PatchCommandType.Remove,
								   Name = "Notes",
								   Value = oldTagName
							   },
							   new PatchRequest
							   {
								   Type = PatchCommandType.Add,
								   Name = "Notes",
								   Value = "new"
							   }
						   }
					   }
				   },
				   false
			   );

				Assert.Equal("{\"Comment\":{\"Notes\":[\"item\",\"new\"]}}", store.DatabaseCommands.Get("items/1").DataAsJson.ToString(Formatting.None));
			}
		}
	}
}