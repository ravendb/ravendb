using System;
using Raven.Database;
using Raven.Database.Indexing;
using Xunit;
using System.Linq;

namespace Raven.Tests.Storage
{
	public class DeleteIndexes : AbstractDocumentStorageTest
	{
		private readonly DocumentDatabase db;

		public DeleteIndexes()
		{
			db = new DocumentDatabase(new RavenConfiguration {DataDirectory = "raven.db.test.esent"});
		}

		#region IDisposable Members

		public override void Dispose()
		{
			db.Dispose();
			base.Dispose();
		}

		#endregion

		[Fact]
		public void Can_remove_index()
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
			db.DeleteIndex("pagesByTitle");
			var indexNames = db.IndexDefinitionStorage.IndexNames.Where(x => x.StartsWith("Raven") == false).ToArray();
			Assert.Equal(0, indexNames.Length);
		}

		[Fact]
		public void Removing_index_remove_it_from_index_storage()
		{
			const string definition =
				@"
    from doc in docs
    where doc.type == ""page""
    select new { Key = doc.title, Value = doc.content, Size = doc.size };
";
			db.PutIndex("pagesByTitle", new IndexDefinition{Map = definition});
			db.DeleteIndex("pagesByTitle");
			var actualDefinition = db.IndexStorage.Indexes.Where(x=>x.StartsWith("Raven") == false);
			Assert.Empty(actualDefinition);
		}
	}
}