using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public async Task UnableToFilterOnNullableDateOnlyProperty(Options options)
    {
        using var store = GetDocumentStore(options);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new Employee {Id = "employees/1", DateTimeDateOfBirth = new DateTime(2000, 1, 1), DateOnlyDateOfBirth = new DateOnly(2000, 1, 1)});
            await session.SaveChangesAsync();
        }

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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
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

    private record ItemWithoutNullValues(string Name, DateOnly? DateOnly, TimeOnly? TimeOnly);
}
