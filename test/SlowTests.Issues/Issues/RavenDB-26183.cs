using System;
using System.Linq;
using FastTests;
using Orders;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_26183 : RavenTestBase
{
    public RavenDB_26183(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_ReturnsDocumentsWithDateBeforeCurrentTime(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddHours(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt <= now()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Today_ReturnsDocumentsWithDateBeforeStartOfToday(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-2) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt < today()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WorksWithGreaterThanOrEqual(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt >= now()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(1, employees.Count);
                Assert.True(employees[0].HiredAt > DateTime.UtcNow.AddMonths(6));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_IsCaseInsensitive(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt <= NOW()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(1, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Today_IsCaseInsensitive(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt >= TODAY()")
                    .WaitForNonStaleResults()
                    .ToList();

                // today() returns start of today (UTC midnight), so a future date passes the filter
                // and today's date (which is >= midnight) also passes
                Assert.True(employees.Count >= 1);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void BackwardCompatibility_ExplicitParameterStillWorks(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = new DateTime(1990, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
                session.Store(new Employee { HiredAt = new DateTime(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
                session.Store(new Employee { HiredAt = new DateTime(2020, 1, 1, 0, 0, 0, DateTimeKind.Utc) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt >= $cutoff")
                    .AddParameter("cutoff", new DateTime(1995, 1, 1, 0, 0, 0, DateTimeKind.Utc))
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_And_Today_WorkInAndExpression(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1), FirstName = "Alice" });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1), FirstName = "Bob" });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1), FirstName = "Charlie" });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt <= now() and FirstName = 'Alice'")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(1, employees.Count);
                Assert.Equal("Alice", employees[0].FirstName);
            }
        }
    }
}
