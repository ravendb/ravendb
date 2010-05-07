using System;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class CreateIndexes : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public CreateIndexes()
		{
			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = "raven.db.test.esent", RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true});
		}

		#region IDisposable Members

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		#endregion

		[Fact]
		public void Index_with_same_name_can_be_added_twice()
		{
			db.PutIndex("pagesByTitle",
						new IndexDefinition
						{
							Map = @"
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});

			db.PutIndex("pagesByTitle",
						new IndexDefinition
						{
							Map = @"
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
						});
		}

		[Fact]
		public void Can_add_index()
		{
			db.PutIndex("pagesByTitle",
			            new IndexDefinition
			            {
							Map = @"
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
			            });
			var indexNames = db.IndexDefinitionStorage.IndexNames.Where(x=>x.StartsWith("Raven") == false).ToArray();
			Assert.Equal(1, indexNames.Length);
			Assert.Equal("pagesByTitle", indexNames[0]);
		}

		[Fact]
		public void Can_list_index_definition()
		{
			const string definition =
				@" 
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = doc.size };
";
			db.PutIndex("pagesByTitle", new IndexDefinition{Map = definition});
			var actualDefinition = db.IndexDefinitionStorage.GetIndexDefinition("pagesByTitle");
			Assert.Equal(definition, actualDefinition.Map);
		}
	}
}