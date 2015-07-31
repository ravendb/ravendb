using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using System.Linq;

namespace Raven.Tests.Bundles.Replication
{
	public class StoreIndex : ReplicationBase
	{
		[Fact]
		public void When_storing_index_replicate_to_all_stores()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

            SetupReplication(store1.DatabaseCommands, store2, store3);

			var index = new IndexSample();
			index.Execute(store1.DatabaseCommands, new DocumentConvention());

			Assert.True(WaitForIndexToReplicate(store2.DatabaseCommands, index.IndexName));
			Assert.True(WaitForIndexToReplicate(store3.DatabaseCommands, index.IndexName));
		}

		[Fact]
		public async Task When_storing_index_replicate_to_all_stores_async()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

            SetupReplication(store1.DatabaseCommands, store2, store3);

			var index = new IndexSample();
		    await index.ExecuteAsync(store1.AsyncDatabaseCommands, new DocumentConvention());

			Assert.True(WaitForIndexToReplicate(store2.DatabaseCommands, index.IndexName));
			Assert.True(WaitForIndexToReplicate(store3.DatabaseCommands, index.IndexName));
		}

        [Fact]
        public void When_storing_index_replicate_to_all_stores_in_respective_databases()
        {
            var store1 = CreateStore(configureStore: store => store.DefaultDatabase = "MasterDb");
            var store2 = CreateStore(configureStore: store => store.DefaultDatabase = "Slave1Db");
            var store3 = CreateStore(configureStore: store => store.DefaultDatabase = "Slave2Db");

            SetupReplication(store1.DatabaseCommands, new[]
            {
                new RavenJObject { { "Url", store2.Url }, { "Database", "Slave1Db" } }, 
                new RavenJObject { { "Url", store3.Url }, { "Database", "Slave2Db" } } 
            });

            var index = new IndexSample();
            index.Execute(store1.DatabaseCommands, new DocumentConvention());

			Assert.True(WaitForIndexToReplicate(store2.DatabaseCommands, index.IndexName));
			Assert.True(WaitForIndexToReplicate(store3.DatabaseCommands, index.IndexName));
        }

        [Fact]
        public async Task When_storing_index_replicate_to_all_stores_in_respective_databases_async()
        {
            var store1 = CreateStore(configureStore: store => store.DefaultDatabase = "MasterDb");
            var store2 = CreateStore(configureStore: store => store.DefaultDatabase = "Slave1Db");
            var store3 = CreateStore(configureStore: store => store.DefaultDatabase = "Slave2Db");

            SetupReplication(store1.DatabaseCommands, new[]
            {
                new RavenJObject { { "Url", store2.Url }, { "Database", "Slave1Db" } }, 
                new RavenJObject { { "Url", store3.Url }, { "Database", "Slave2Db" } } 
            });

            var index = new IndexSample();
            await index.ExecuteAsync(store1.AsyncDatabaseCommands, new DocumentConvention());

			Assert.True(WaitForIndexToReplicate(store2.DatabaseCommands, index.IndexName));
			Assert.True(WaitForIndexToReplicate(store3.DatabaseCommands, index.IndexName));
        }

		[Fact]
		public void Replicate_all_indexes_created_by_execute_method_call()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2, store3);

			Thread.Sleep(5000); // let it start and complete the initial execution of the index and transformer replication tasks

			for (int i = 0; i < 10; i++)
			{
				var index = new IndexSample(i.ToString());
				index.Execute(store1.DatabaseCommands, new DocumentConvention());
			}

			for (int i = 0; i < 10; i++)
			{
				Assert.True(WaitForIndexToReplicate(store2.DatabaseCommands, i.ToString()));
				Assert.True(WaitForIndexToReplicate(store3.DatabaseCommands, i.ToString()));
			}	
		}

	}

	class IndexSample : AbstractIndexCreationTask
	{
		private readonly string name;

		public IndexSample(string name = "TestIndex")
		{
			this.name = name;
		}

		public override string IndexName
		{
			get { return name; }
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