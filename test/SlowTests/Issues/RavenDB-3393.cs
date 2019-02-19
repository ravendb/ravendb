using System;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_3393 : RavenTestBase
    {
        [Fact]
        public void get_statistics_for_database_command()
        {
            using (var store = GetDocumentStore())
            {
                store.Initialize();

                var dbName = $"get_statistics_for_database_command-{Guid.NewGuid()}";
                store
                    .Maintenance
                    .Server
                    .Send(new CreateDatabaseOperation(new DatabaseRecord(dbName)));

                using (EnsureDatabaseDeletion(dbName, store))
                using (var session = store.OpenSession(dbName))
                {
                    session.Store(new Car { Brand = "Toyota", Color = "Silver", Year = 2005 });
                    session.Store(new Car { Brand = "Toyota", Color = "Silver", Year = 2008 });
                    session.Store(new Car { Brand = "Mazda", Color = "Red", Year = 2015 });
                    session.Store(new Car { Brand = "Mazda", Color = "Red", Year = 2011 });

                    session.SaveChanges();

                    var sysStats = store.Maintenance.Send(new GetStatisticsOperation());
                    var dbStats = store.Maintenance.ForDatabase(dbName).Send(new GetStatisticsOperation());
                    var dbStats2 = store.Maintenance.ForDatabase(dbName).ForDatabase(dbName).Send(new GetStatisticsOperation());
                    Assert.Equal(dbStats.DatabaseId, dbStats2.DatabaseId);
                    Assert.NotEqual(dbStats2.DatabaseId, sysStats.DatabaseId);
                    Assert.NotEqual(dbStats.DatabaseId, sysStats.DatabaseId);
                }
            }
        }
        private class Car
        {
            public string Brand { get; set; }
            public string Color { get; set; }
            public int Year { get; set; }
        }
    }
}
