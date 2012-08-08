using Raven.Abstractions.Indexing;
using Raven.Bundles.Tests.Replication;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Xunit;
using System.Linq;

namespace Raven.Tests.Bundles.Replication
{
	class StoreIndex : ReplicationBase
	{
		[Fact]
		public void When_storeing_index_replicate_to_all_stores()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2.Url, store3.Url);

			var index = new IndexSample();
			index.Execute(store1.DatabaseCommands, new DocumentConvention());

			Assert.True(store2.DatabaseCommands.GetIndexNames(0, 10).ToList().Contains(index.IndexName));
			Assert.True(store3.DatabaseCommands.GetIndexNames(0, 10).ToList().Contains(index.IndexName));
		}

		[Fact]
		public void When_storeing_index_replicate_to_all_stores_async()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2.Url, store3.Url);

			var index = new IndexSample();
			index.ExecuteAsync(store1.AsyncDatabaseCommands, new DocumentConvention()).ContinueWith(task =>
			{
				Assert.True(store2.DatabaseCommands.GetIndexNames(0, 10).ToList().Contains(index.IndexName));
				Assert.True(store3.DatabaseCommands.GetIndexNames(0, 10).ToList().Contains(index.IndexName));
			}).Wait();
		}
	}

	class IndexSample : AbstractIndexCreationTask
	{
		public override string IndexName
		{
			get
			{
				return "TestIndex";
			}
		}
		public override IndexDefinition CreateIndexDefinition()
		{
			return new IndexDefinition()
			{

				Map =
					@"
	from doc in docs
	where doc.type == ""page""
	select new { Key = doc.title, Value = doc.content, Size = doc.size };
"
			};
		}
	}
}