using Raven.Abstractions.Data;
using Raven.Client.Embedded;
using Raven.Client.Extensions;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_3393 : RavenTestBase
	{
		[Fact]
		public void get_statistics_for_database_command()
		{
			using (var store = new EmbeddableDocumentStore {RunInMemory = true})
			{
				store.Initialize();

				store
					.DatabaseCommands
					.GlobalAdmin
					.CreateDatabase(new DatabaseDocument
					{
						Id = "Db",
						Settings =
						{
							{"Raven/DataDir", "Db"}
						}
					});
				store.DatabaseCommands.EnsureDatabaseExists("Db");

				using (var session = store.OpenSession("Db"))
				{
					session.Store(new Car {Brand = "Toyota", Color = "Silver", Year = 2005});
					session.Store(new Car {Brand = "Toyota", Color = "Silver", Year = 2008});
					session.Store(new Car {Brand = "Mazda", Color = "Red", Year = 2015});
					session.Store(new Car {Brand = "Mazda", Color = "Red", Year = 2011});

					session.SaveChanges();

					var sysStats = store.DatabaseCommands.GetStatistics();
					var dbStats = store.DatabaseCommands.ForDatabase("Db").GetStatistics();
					var dbStats2 = store.DatabaseCommands.ForDatabase("Db").ForDatabase("Db").GetStatistics();
					Assert.Equal(dbStats.DatabaseId, dbStats2.DatabaseId);
				    Assert.NotEqual(dbStats2.DatabaseId, sysStats.DatabaseId);
					Assert.NotEqual(dbStats.DatabaseId, sysStats.DatabaseId);
				}
			}
		}
		public class Car
		{
			public string Brand { get; set; }
			public string Color { get; set; }
			public int Year { get; set; }

		}
	}
}