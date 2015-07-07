using System;
using System.Collections.Generic;
using System.ComponentModel.Composition.Hosting;
using System.Linq;
using System.Reflection;
using System.Security.Policy;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;
using Raven.Tests.Misc;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3625: RavenTest
	{
		[Fact]
		public void CanExecuteMultipleIndexes()
		{
			using (var store = NewDocumentStore(databaseName:"MultiIndexes"))
			{
				IndexCreation.CreateIndexes(Assembly.GetAssembly(typeof(TestStrIndex)), store);
				var names = store.DatabaseCommands.GetIndexNames(0,3);
				Assert.Contains("TestIntIndex", names);
				Assert.Contains("TestStrIndex", names);
			}
		}
		[Fact]
		public void CanExecuteMultipleIndexesWithConventions()
		{
			using (var store = NewDocumentStore(databaseName: "MultiIndexes"))
			{
				IndexCreation.CreateIndexes(new CompositionContainer(new AssemblyCatalog(Assembly.GetAssembly(typeof(TestStrIndex)))), store.DatabaseCommands, new DocumentConvention());
				var names = store.DatabaseCommands.GetIndexNames(0, 3);
				Assert.Contains("TestIntIndex", names);
				Assert.Contains("TestStrIndex", names);
			}
		}
		[Fact]
		public async Task CanExecuteMultipleIndexesAsync()
		{
			using (var store = NewDocumentStore(databaseName: "MultiIndexes"))
			{
				await IndexCreation.CreateIndexesAsync(Assembly.GetAssembly(typeof(TestStrIndex)), store).ConfigureAwait(false);
				var names = store.DatabaseCommands.GetIndexNames(0, 3);
				Assert.Contains("TestIntIndex", names);
				Assert.Contains("TestStrIndex", names);
			}
		}
		[Fact]
		public async Task CanExecuteMultipleIndexesWithConventionsAsync()
		{
			using (var store = NewDocumentStore(databaseName: "MultiIndexes"))
			{
				await IndexCreation.CreateIndexesAsync(new CompositionContainer(new AssemblyCatalog(Assembly.GetAssembly(typeof(TestStrIndex)))), store.AsyncDatabaseCommands, new DocumentConvention()).ConfigureAwait(false);
				var names = store.DatabaseCommands.GetIndexNames(0, 3);
				Assert.Contains("TestIntIndex", names);
				Assert.Contains("TestStrIndex", names);
			}
		}

	}
}
