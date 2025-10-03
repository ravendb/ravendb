using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19205 : RavenTestBase
{
    public RavenDB_19205(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public async Task UnableToFilterOnNullableDateOnlyProperty(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Employee {Id = "employees/1", DateTimeDateOfBirth = new DateTime(2000, 1, 1), DateOnlyDateOfBirth = new DateOnly(2000, 1, 1)});
            await session.SaveChangesAsync();
        }

        WaitForUserToContinueTheTest(store);
        // Querying the DateTime property works fine.
        using (var session = store.OpenAsyncSession())
        {
            var employees = await session
                .Query<Employee>()
                .Customize(i => i.WaitForNonStaleResults())
                .Where(e => e.DateTimeDateOfBirth != null && e.DateTimeDateOfBirth.Value.Year == 2000)
                .ToListAsync();

            Assert.Equal(1, employees.Count);
        }


        // Querying the DateOnly property returns no results.
        using (var session = store.OpenAsyncSession())
        {
            var employees = await session
                .Query<Employee>()
                .Customize(i => i.WaitForNonStaleResults())
                .Where(e => e.DateOnlyDateOfBirth != null && e.DateOnlyDateOfBirth.Value.Year == 2000)
                .ToListAsync();
            Assert.Equal(1, employees.Count);
        }
    }

    private class Employee
    {
        public string Id { get; init; }
        public DateTime? DateTimeDateOfBirth { get; init; }
        public DateOnly? DateOnlyDateOfBirth { get; init; }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void CanProduceTicksFromTimeOnlyDateOnlyViaAutoIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        var initialDate = new DateTime(2023, 3, 6, 12, 5, 0);
        using (var session = store.OpenSession())
        {
            session.Store(new ItemWithoutNullValues("Data1", DateOnly.FromDateTime(initialDate), TimeOnly.FromDateTime(initialDate)));
            session.SaveChanges();
        }

        using (var session = store.OpenSession())
        {
            // ReSharper disable once ReplaceWithSingleCallToCount
            var result = session
                .Query<ItemWithoutNullValues>()
                .Customize(i => i.WaitForNonStaleResults())
                .Where(item => item.DateOnly != null
                               && item.DateOnly.Value.Year == 2023
                               && item.TimeOnly != null
                               && item.TimeOnly.Value.Minute == 5)
                .Count();

            WaitForUserToContinueTheTest(store);
            Assert.Equal(1L, result);
        }
    }

    [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void BackwardCompatibilityForDateOnlyInAutoIndexes(Options options)
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var fullBackupPath = Path.Combine(backupPath, "RavenDB_19205.ravendb-snapshot");

        ExtractFile(fullBackupPath);
        using (var store = GetDocumentStore(options))
        {
            var databaseName = GetDatabaseName();

            using (Backup.RestoreDatabase(store, new RestoreBackupConfiguration {BackupLocation = backupPath, DatabaseName = databaseName}))
            {
                using (var session = store.OpenSession(databaseName))
                {
                    var result = session
                        .Query<Employee>()
                        .Count(i => i.DateOnlyDateOfBirth < new DateOnly(2022, 1, 1));
                    Assert.Equal(1, result);

                    session.Store(new Employee() {DateOnlyDateOfBirth = new(2019, 1, 1)});

                    session.SaveChanges();

                    result = session.Query<Employee>()
                        .Customize(i => i.WaitForNonStaleResults().NoCaching().NoTracking())
                        .Count(i => i.DateOnlyDateOfBirth < new DateOnly(2022, 1, 1));

                    Assert.Equal(2, result);
                }
            }
        }
    }

    private record ItemWithoutNullValues(string Name, DateOnly? DateOnly, TimeOnly? TimeOnly);

    private static void ExtractFile(string path)
    {
        using (var file = File.Create(path))
        using (var stream = typeof(RavenDB_19205).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_19205.RavenDB_19205.ravendb-snapshot"))
        {
            stream.CopyTo(file);
        }
    }
}
