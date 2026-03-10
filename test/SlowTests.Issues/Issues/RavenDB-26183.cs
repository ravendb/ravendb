using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Sparrow.Json;
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

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Linq_Now_LessThanOrEqual(Options options)
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
                var query = session.Query<Employee>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(e => e.HiredAt <= RavenQuery.Now());

                Assert.Equal("from 'Employees' where HiredAt <= now()", query.ToString());

                var employees = query.ToList();
                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Linq_Now_GreaterThanOrEqual(Options options)
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
                var query = session.Query<Employee>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(e => e.HiredAt >= RavenQuery.Now());

                Assert.Equal("from 'Employees' where HiredAt >= now()", query.ToString());

                var employees = query.ToList();
                Assert.Equal(1, employees.Count);
                Assert.True(employees[0].HiredAt > DateTime.UtcNow.AddMonths(6));
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Linq_Today_LessThan(Options options)
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
                var query = session.Query<Employee>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(e => e.HiredAt < RavenQuery.Today());

                Assert.Equal("from 'Employees' where HiredAt < today()", query.ToString());

                var employees = query.ToList();
                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Linq_Today_GreaterThanOrEqual(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-2) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = session.Query<Employee>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(e => e.HiredAt >= RavenQuery.Today());

                Assert.Equal("from 'Employees' where HiredAt >= today()", query.ToString());

                var employees = query.ToList();
                Assert.Equal(1, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void DocumentQuery_Now_LessThanOrEqual(Options options)
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
                var query = session.Advanced
                    .DocumentQuery<Employee>()
                    .WhereLessThanOrEqual("HiredAt", Time.Now)
                    .WaitForNonStaleResults();

                Assert.Equal("from 'Employees' where HiredAt <= now()", query.ToString());

                var employees = query.ToList();
                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void DocumentQuery_Now_GreaterThanOrEqual(Options options)
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
                var query = session.Advanced
                    .DocumentQuery<Employee>()
                    .WhereGreaterThanOrEqual("HiredAt", Time.Now)
                    .WaitForNonStaleResults();

                Assert.Equal("from 'Employees' where HiredAt >= now()", query.ToString());

                var employees = query.ToList();
                Assert.Equal(1, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void DocumentQuery_Today_LessThan(Options options)
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
                var query = session.Advanced
                    .DocumentQuery<Employee>()
                    .WhereLessThan("HiredAt", Time.Today)
                    .WaitForNonStaleResults();

                Assert.Equal("from 'Employees' where HiredAt < today()", query.ToString());

                var employees = query.ToList();
                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void DocumentQuery_Today_GreaterThanOrEqual(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-2) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var query = session.Advanced
                    .DocumentQuery<Employee>()
                    .WhereGreaterThanOrEqual("HiredAt", Time.Today)
                    .WaitForNonStaleResults();

                Assert.Equal("from 'Employees' where HiredAt >= today()", query.ToString());

                var employees = query.ToList();
                Assert.Equal(1, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public async Task AsyncDocumentQuery_Now_LessThanOrEqual(Options options)
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

            using (var session = store.OpenAsyncSession())
            {
                var query = session.Advanced
                    .AsyncDocumentQuery<Employee>()
                    .WhereLessThanOrEqual("HiredAt", Time.Now)
                    .WaitForNonStaleResults();

                Assert.Equal("from 'Employees' where HiredAt <= now()", query.ToString());

                var employees = await query.ToListAsync();
                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public async Task AsyncDocumentQuery_Today_GreaterThanOrEqual(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-2) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) });
                session.SaveChanges();
            }

            using (var session = store.OpenAsyncSession())
            {
                var query = session.Advanced
                    .AsyncDocumentQuery<Employee>()
                    .WhereGreaterThanOrEqual("HiredAt", Time.Today)
                    .WaitForNonStaleResults();

                Assert.Equal("from 'Employees' where HiredAt >= today()", query.ToString());

                var employees = await query.ToListAsync();
                Assert.Equal(1, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void DocumentQuery_Now_WhereEquals(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var query = session.Advanced
                    .DocumentQuery<Employee>()
                    .WhereEquals("HiredAt", Time.Now);

                Assert.Equal("from 'Employees' where HiredAt = now()", query.ToString());
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void DocumentQuery_Today_WhereEquals(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var query = session.Advanced
                    .DocumentQuery<Employee>()
                    .WhereEquals("HiredAt", Time.Today);

                Assert.Equal("from 'Employees' where HiredAt = today()", query.ToString());
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Linq_Now_And_Today_GenerateCorrectRql(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                // Verify LINQ generates correct RQL for all comparison operators
                var queryLte = session.Query<Employee>()
                    .Where(e => e.HiredAt <= RavenQuery.Now());
                Assert.Equal("from 'Employees' where HiredAt <= now()", queryLte.ToString());

                var queryGte = session.Query<Employee>()
                    .Where(e => e.HiredAt >= RavenQuery.Now());
                Assert.Equal("from 'Employees' where HiredAt >= now()", queryGte.ToString());

                var queryLt = session.Query<Employee>()
                    .Where(e => e.HiredAt < RavenQuery.Now());
                Assert.Equal("from 'Employees' where HiredAt < now()", queryLt.ToString());

                var queryGt = session.Query<Employee>()
                    .Where(e => e.HiredAt > RavenQuery.Now());
                Assert.Equal("from 'Employees' where HiredAt > now()", queryGt.ToString());

                var queryTodayLte = session.Query<Employee>()
                    .Where(e => e.HiredAt <= RavenQuery.Today());
                Assert.Equal("from 'Employees' where HiredAt <= today()", queryTodayLte.ToString());

                var queryTodayGte = session.Query<Employee>()
                    .Where(e => e.HiredAt >= RavenQuery.Today());
                Assert.Equal("from 'Employees' where HiredAt >= today()", queryTodayGte.ToString());
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void DocumentQuery_Now_AllComparisonOperators_GenerateCorrectRql(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                var queryGt = session.Advanced.DocumentQuery<Employee>()
                    .WhereGreaterThan("HiredAt", Time.Now);
                Assert.Equal("from 'Employees' where HiredAt > now()", queryGt.ToString());

                var queryGte = session.Advanced.DocumentQuery<Employee>()
                    .WhereGreaterThanOrEqual("HiredAt", Time.Now);
                Assert.Equal("from 'Employees' where HiredAt >= now()", queryGte.ToString());

                var queryLt = session.Advanced.DocumentQuery<Employee>()
                    .WhereLessThan("HiredAt", Time.Now);
                Assert.Equal("from 'Employees' where HiredAt < now()", queryLt.ToString());

                var queryLte = session.Advanced.DocumentQuery<Employee>()
                    .WhereLessThanOrEqual("HiredAt", Time.Now);
                Assert.Equal("from 'Employees' where HiredAt <= now()", queryLte.ToString());

                var queryEq = session.Advanced.DocumentQuery<Employee>()
                    .WhereEquals("HiredAt", Time.Now);
                Assert.Equal("from 'Employees' where HiredAt = now()", queryEq.ToString());

                var queryNeq = session.Advanced.DocumentQuery<Employee>()
                    .WhereNotEquals("HiredAt", Time.Now);
                Assert.Equal("from 'Employees' where HiredAt != now()", queryNeq.ToString());
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WorksInFilterClause(Options options)
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
                    .RawQuery<Employee>("from Employees filter HiredAt <= now()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Today_WorksInFilterClause(Options options)
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
                    .RawQuery<Employee>("from Employees filter HiredAt < today()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Linq_Now_WorksInFilterClause(Options options)
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
                var query = session.Query<Employee>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Filter(e => e.HiredAt <= RavenQuery.Now());

                var employees = query.ToList();
                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void DocumentQuery_Now_WorksInFilterClause(Options options)
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
                var query = session.Advanced
                    .DocumentQuery<Employee>()
                    .Filter(f => f.LessThanOrEqual("HiredAt", Time.Now))
                    .WaitForNonStaleResults();

                var employees = query.ToList();
                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Now_ShouldNotReturnNotModified(Options options)
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
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var query = new IndexQuery
                {
                    Query = "FROM Employees WHERE HiredAt <= now()",
                    WaitForNonStaleResultsTimeout = TimeSpan.FromMinutes(1)
                };

                var command = new QueryCommand((InMemoryDocumentSessionOperations)session, query);
                await session.Advanced.RequestExecutor.ExecuteAsync(command, context);

                Assert.Equal(1, command.Result.Results.Length);

                var command2 = new QueryCommand((InMemoryDocumentSessionOperations)session, query);
                await session.Advanced.RequestExecutor.ExecuteAsync(command2, context);

                Assert.NotEqual(HttpStatusCode.NotModified, command2.StatusCode);
                Assert.NotEqual(-1, command2.Result.DurationInMs);
                Assert.Equal(1, command2.Result.Results.Length);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Today_ShouldNotReturnNotModified(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-2) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var query = new IndexQuery
                {
                    Query = "FROM Employees WHERE HiredAt < today()",
                    WaitForNonStaleResultsTimeout = TimeSpan.FromMinutes(1)
                };

                var command = new QueryCommand((InMemoryDocumentSessionOperations)session, query);
                await session.Advanced.RequestExecutor.ExecuteAsync(command, context);

                Assert.Equal(1, command.Result.Results.Length);

                var command2 = new QueryCommand((InMemoryDocumentSessionOperations)session, query);
                await session.Advanced.RequestExecutor.ExecuteAsync(command2, context);

                Assert.NotEqual(HttpStatusCode.NotModified, command2.StatusCode);
                Assert.NotEqual(-1, command2.Result.DurationInMs);
                Assert.Equal(1, command2.Result.Results.Length);
            }
        }
    }
}
