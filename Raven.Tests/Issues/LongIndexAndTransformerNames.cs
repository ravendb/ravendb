// -----------------------------------------------------------------------
//  <copyright file="LongIndexAndTransformerNames.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
namespace Raven.Tests.Issues
{
	using System.Threading.Tasks;

	using Raven.Abstractions.Data;
	using Raven.Abstractions.Indexing;

	using Xunit;

	public class LongIndexAndTransformerNames : RavenTest
	{
		[Fact]
		public void SuperLongIndexName()
		{
			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				var name = new string('a', 200);

				store.DatabaseCommands.PutIndex(name, new IndexDefinition { Name = name, Map = "from doc in docs select new { Name = doc.Name }" });

				var i = store.DatabaseCommands.GetIndex(name);

				Assert.NotNull(i);
				Assert.Equal(name, i.Name);

				store.DatabaseCommands.DeleteIndex(name);

				i = store.DatabaseCommands.GetIndex(name);

				Assert.Null(i);
			}
		}

		[Fact]
		public void SuperLongTransformerName()
		{
			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				var name = new string('a', 200);

				store.DatabaseCommands.PutTransformer(name, new TransformerDefinition { Name = name, TransformResults = "from doc in results select new { Name = doc.Name }" });

				var t = store.DatabaseCommands.GetTransformer(name);

				Assert.NotNull(t);
				Assert.Equal(name, t.Name);

				store.DatabaseCommands.DeleteTransformer(name);

				t = store.DatabaseCommands.GetTransformer(name);

				Assert.Null(t);
			}
		}

		[Fact]
		public async Task SuperLongIndexNameAsync()
		{
			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				var name = new string('a', 200);

				await store.AsyncDatabaseCommands.PutIndexAsync(name, new IndexDefinition { Name = name, Map = "from doc in docs select new { Name = doc.Name }" }, false);

				var i = await store.AsyncDatabaseCommands.GetIndexAsync(name);

				Assert.NotNull(i);
				Assert.Equal(name, i.Name);

				await store.AsyncDatabaseCommands.DeleteIndexAsync(name);

				i = await store.AsyncDatabaseCommands.GetIndexAsync(name);

				Assert.Null(i);
			}
		}

		[Fact]
		public async Task SuperLongTransformerNameAsync()
		{
			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				var name = new string('a', 200);

				await store.AsyncDatabaseCommands.PutTransformerAsync(name, new TransformerDefinition { Name = name, TransformResults = "from doc in results select new { Name = doc.Name }" });

				var t = await store.AsyncDatabaseCommands.GetTransformerAsync(name);

				Assert.NotNull(t);
				Assert.Equal(name, t.Name);

				await store.AsyncDatabaseCommands.DeleteTransformerAsync(name);

				t = await store.AsyncDatabaseCommands.GetTransformerAsync(name);

				Assert.Null(t);
			}
		}

		[Fact]
		public void QueryingSuperLongIndexName()
		{
			using (var store = NewDocumentStore())
			{
				var name = new string('a', 200);

				store.DatabaseCommands.PutIndex(name, new IndexDefinition { Name = name, Map = "from doc in docs select new { Name = doc.Name }" });

				Assert.DoesNotThrow(() => store.DatabaseCommands.Query(name, new IndexQuery(), null)); 
			}

			using (var store = NewDocumentStore(runInMemory: false))
			{
				var name = new string('a', 200);

				store.DatabaseCommands.PutIndex(name, new IndexDefinition { Name = name, Map = "from doc in docs select new { Name = doc.Name }" });

				Assert.DoesNotThrow(() => store.DatabaseCommands.Query(name, new IndexQuery(), null));
			}

			using (var store = NewRemoteDocumentStore())
			{
				var name = new string('a', 200);

				store.DatabaseCommands.PutIndex(name, new IndexDefinition { Name = name, Map = "from doc in docs select new { Name = doc.Name }" });

				Assert.DoesNotThrow(() => store.DatabaseCommands.Query(name, new IndexQuery(), null));
			}

			using (var store = NewRemoteDocumentStore(runInMemory: false))
			{
				var name = new string('a', 200);

				store.DatabaseCommands.PutIndex(name, new IndexDefinition { Name = name, Map = "from doc in docs select new { Name = doc.Name }" });

				Assert.DoesNotThrow(() => store.DatabaseCommands.Query(name, new IndexQuery(), null));
			}

			using (var store = NewRemoteDocumentStore(requestedStorage: "esent"))
			{
				var name = new string('a', 200);

				store.DatabaseCommands.PutIndex(name, new IndexDefinition { Name = name, Map = "from doc in docs select new { Name = doc.Name }" });

				Assert.DoesNotThrow(() => store.DatabaseCommands.Query(name, new IndexQuery(), null));
			}
		}
	}
}