using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;
using Raven.Client.Document;

namespace Raven.Tests.Issues
{
	public class RavenDB_3530 : RavenTestBase
	{
		[Fact]
		public void settingCompileIndexCacheDirectoryWithSettingsNVC()
		{
			const string databaseRoot = "Raven";
			string dataDirectory = Path.Combine(databaseRoot, "Database");
			string compiledIndexCacheDirectory = Path.Combine(databaseRoot, "CompiledIndexCache");
			using (var embeddedStore = new EmbeddableDocumentStore
			{
				//DefaultDatabase = "testApp",
				UseEmbeddedHttpServer = true,
				RunInMemory = false,
				DataDirectory = dataDirectory,
				Configuration =
				{
					Settings = new NameValueCollection
					{
						{"Raven/CompiledIndexCacheDirectory", compiledIndexCacheDirectory} // THIS WORKS!
					}
				}

			})
			{
				embeddedStore.Configuration.Port = 8079;

				embeddedStore.Configuration.CompiledIndexCacheDirectory = compiledIndexCacheDirectory; 

				embeddedStore.Initialize();

				var localStore = new DocumentStore()
				{
					Url = "http://localhost:8079",
					DefaultDatabase = "testDB"
				};
				localStore.Initialize();

				localStore.DatabaseCommands.EnsureDatabaseExists("testDB");


				localStore.ExecuteIndex(new Index());
				using (var session = localStore.OpenSession())
				{
					session.Store(new Person{Name = "Hila", Age = 29});
					session.Store(new Person { Name = "John", Age = 50 });
					session.SaveChanges();
					var persons= session.Query<Index>().ToString();

				} 
				var path = embeddedStore.Configuration.CompiledIndexCacheDirectory;
				var directoryOfCompiledIndexExsits = Directory.Exists(path);
				Assert.True(directoryOfCompiledIndexExsits); Assert.Equal("Raven\\CompiledIndexCache", compiledIndexCacheDirectory);
			}
		}

		[Fact]
		public void settingCompileIndexCacheDirectoryUsingConfiguration()
		{
			const string databaseRoot = "Raven";
			string dataDirectory = Path.Combine(databaseRoot, "Database");
			string compiledIndexCacheDirectory = Path.Combine(databaseRoot, "CompiledIndexCache");
			using (var embeddedStore = new EmbeddableDocumentStore
			{
				//DefaultDatabase = "testApp",
				UseEmbeddedHttpServer = true,
				RunInMemory = false,
				DataDirectory = dataDirectory
			})
			{
				embeddedStore.Configuration.Port = 8079;

				embeddedStore.Configuration.CompiledIndexCacheDirectory = compiledIndexCacheDirectory;

				embeddedStore.Initialize();

				var localStore = new DocumentStore()
				{
					Url = "http://localhost:8079",
					DefaultDatabase = "testDB"
				};
				localStore.Initialize();

				localStore.DatabaseCommands.EnsureDatabaseExists("testDB");


				localStore.ExecuteIndex(new Index());
				using (var session = localStore.OpenSession())
				{
					session.Store(new Person { Name = "Hila", Age = 29 });
					session.Store(new Person { Name = "John", Age = 50 });
					session.SaveChanges();
					var persons = session.Query<Index>().ToString();

				}
				var path = embeddedStore.Configuration.CompiledIndexCacheDirectory;
				var directoryOfCompiledIndexExsits = Directory.Exists(path);
				Assert.True(directoryOfCompiledIndexExsits);
			}
		}
		[Fact]
		public void settingCompileIndexCacheDirectoryWitoutSettingsNVC()
		{
			const string databaseRoot = "Raven";
			string dataDirectory = Path.Combine(databaseRoot, "Database");
			string compiledIndexCacheDirectory = Path.Combine(databaseRoot, "CompiledIndexCache");
			using (var embeddedStore = new EmbeddableDocumentStore
			{
				//DefaultDatabase = "testApp",
				UseEmbeddedHttpServer = true,
				RunInMemory = false,
				DataDirectory = dataDirectory,
				Configuration =
				{
					CompiledIndexCacheDirectory = compiledIndexCacheDirectory
				}

			})
			{
				embeddedStore.Configuration.Port = 8079;

				embeddedStore.Initialize();

				var localStore = new DocumentStore()
				{
					Url = "http://localhost:8079",
					DefaultDatabase = "testDB"
				};
				localStore.Initialize();
				localStore.DatabaseCommands.EnsureDatabaseExists("testDB");
				localStore.ExecuteIndex(new Index());
				using (var session = localStore.OpenSession())
				{
					session.Store(new Person { Name = "Hila", Age = 29 });
					session.Store(new Person { Name = "John", Age = 50 });
					session.SaveChanges();
					var persons = session.Query<Index>().ToString();

				}
				var path = embeddedStore.Configuration.CompiledIndexCacheDirectory;
				var directoryOfCompiledIndexExsits = Directory.Exists(path);
				Assert.True(directoryOfCompiledIndexExsits);
			}
		}
		public class Person
		{
			public string Name;
			public int Age;
		}

		public class Index : AbstractIndexCreationTask<Person>
		{
			public Index()
			{
				Map = persons =>
					from person in persons
					select new {person.Name, person.Age};
			}
		}
	}
}
