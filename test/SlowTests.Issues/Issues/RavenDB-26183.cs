using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26183 : RavenTestBase
{
    public RavenDB_26183(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax, DatabaseMode = RavenDatabaseMode.Single)]
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
                
                var employeesWithWhenOperator = session.Advanced
                    .RawQuery<Employee>("from Employees where when($p0 > 10, HiredAt <= now())")
                    .WaitForNonStaleResults()
                    .AddParameter("p0", 20)
                    .ToList();
                Assert.Equal(2, employeesWithWhenOperator.Count);
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
                
                var employeesWithWhenOperator = session.Advanced
                    .RawQuery<Employee>("from Employees where when($p0 > 10, HiredAt < today())")
                    .AddParameter("p0", 20)
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employeesWithWhenOperator.Count);
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
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-2) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt < TODAY()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
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
                    .WhereLessThanOrEqual("HiredAt", RavenDocumentQuery.Now())
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
                    .WhereGreaterThanOrEqual("HiredAt", RavenDocumentQuery.Now())
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
                    .WhereLessThan("HiredAt", RavenDocumentQuery.Today())
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
                    .WhereGreaterThanOrEqual("HiredAt", RavenDocumentQuery.Today())
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
                    .WhereLessThanOrEqual("HiredAt", RavenDocumentQuery.Now())
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
                    .WhereGreaterThanOrEqual("HiredAt", RavenDocumentQuery.Today())
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
                    .WhereEquals("HiredAt", RavenDocumentQuery.Now());

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
                    .WhereEquals("HiredAt", RavenDocumentQuery.Today());

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
                    .WhereGreaterThan("HiredAt", RavenDocumentQuery.Now());
                Assert.Equal("from 'Employees' where HiredAt > now()", queryGt.ToString());

                var queryGte = session.Advanced.DocumentQuery<Employee>()
                    .WhereGreaterThanOrEqual("HiredAt", RavenDocumentQuery.Now());
                Assert.Equal("from 'Employees' where HiredAt >= now()", queryGte.ToString());

                var queryLt = session.Advanced.DocumentQuery<Employee>()
                    .WhereLessThan("HiredAt", RavenDocumentQuery.Now());
                Assert.Equal("from 'Employees' where HiredAt < now()", queryLt.ToString());

                var queryLte = session.Advanced.DocumentQuery<Employee>()
                    .WhereLessThanOrEqual("HiredAt", RavenDocumentQuery.Now());
                Assert.Equal("from 'Employees' where HiredAt <= now()", queryLte.ToString());

                var queryEq = session.Advanced.DocumentQuery<Employee>()
                    .WhereEquals("HiredAt", RavenDocumentQuery.Now());
                Assert.Equal("from 'Employees' where HiredAt = now()", queryEq.ToString());

                var queryNeq = session.Advanced.DocumentQuery<Employee>()
                    .WhereNotEquals("HiredAt", RavenDocumentQuery.Now());
                Assert.Equal("from 'Employees' where HiredAt != now()", queryNeq.ToString());
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_ShouldThrowInFilterClause(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var e = Assert.Throws<RavenException>(() =>
                    session.Advanced
                        .RawQuery<Employee>("from Employees filter HiredAt <= now()")
                        .WaitForNonStaleResults()
                        .ToList());
                Assert.Contains("not supported in filter", e.Message);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Today_ShouldThrowInFilterClause(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var e = Assert.Throws<RavenException>(() =>
                    session.Advanced
                        .RawQuery<Employee>("from Employees filter HiredAt < today()")
                        .WaitForNonStaleResults()
                        .ToList());
                Assert.Contains("not supported in filter", e.Message);
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
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void Sharded_Now_ReturnsConsistentResultsAcrossShards(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var config = Sharding.GetShardingConfiguration(store);

            // place one past and one future employee on each of the 3 shards
            for (int shard = 0; shard < 3; shard++)
            {
                var pastId = Sharding.GetRandomIdForShard(config, shard);
                var futureId = Sharding.GetRandomIdForShard(config, shard);

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) }, pastId);
                    session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) }, futureId);
                    session.SaveChanges();
                }
            }

            using (var session = store.OpenSession())
            {
                var past = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt <= now()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(3, past.Count);
            }

            using (var session = store.OpenSession())
            {
                var future = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt > now()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(3, future.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void Sharded_Today_ReturnsConsistentResultsAcrossShards(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var config = Sharding.GetShardingConfiguration(store);

            // place one old and one future employee on each of the 3 shards
            for (int shard = 0; shard < 3; shard++)
            {
                var oldId = Sharding.GetRandomIdForShard(config, shard);
                var futureId = Sharding.GetRandomIdForShard(config, shard);

                using (var session = store.OpenSession())
                {
                    session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-2) }, oldId);
                    session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) }, futureId);
                    session.SaveChanges();
                }
            }

            using (var session = store.OpenSession())
            {
                var past = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt < today()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(3, past.Count);
            }

            using (var session = store.OpenSession())
            {
                var future = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt >= today()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(3, future.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded)]
    public void Sharded_Now_And_Today_InSameQuery(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var config = Sharding.GetShardingConfiguration(store);

            // place employees on different shards with varying dates
            for (int shard = 0; shard < 3; shard++)
            {
                var matchId = Sharding.GetRandomIdForShard(config, shard);
                var tooOldId = Sharding.GetRandomIdForShard(config, shard);
                var tooNewId = Sharding.GetRandomIdForShard(config, shard);

                using (var session = store.OpenSession())
                {
                    // matches: hired today, guaranteed to be after today's midnight
                    session.Store(new Employee { HiredAt = DateTime.UtcNow.Date.AddMinutes(1) }, matchId);
                    // too old: hired before today
                    session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-2) }, tooOldId);
                    // too new: hired in the future
                    session.Store(new Employee { HiredAt = DateTime.UtcNow.AddYears(1) }, tooNewId);
                    session.SaveChanges();
                }
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt >= today() and HiredAt <= now()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(3, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Filter_WithBothNowAndToday_ShouldThrow(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var e = Assert.Throws<RavenException>(() =>
                    session.Advanced
                        .RawQuery<Employee>("from Employees filter HiredAt >= today() and HiredAt <= now()")
                        .WaitForNonStaleResults()
                        .ToList());
                Assert.Contains("not supported in filter", e.Message);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Subscription_ShouldThrowOnNow(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var exception = await Assert.ThrowsAsync<RavenException>(() =>
                store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Employees where HiredAt <= now()"
                }));

            Assert.Contains("now()", exception.Message);
            Assert.Contains("not supported in filter or subscription expressions", exception.Message);
        }
    }

    [RavenTheory(RavenTestCategory.Subscriptions)]
    [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
    public async Task Subscription_ShouldThrowOnToday(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            var exception = await Assert.ThrowsAsync<RavenException>(() =>
                store.Subscriptions.CreateAsync(new SubscriptionCreationOptions
                {
                    Query = "from Employees where HiredAt < today()"
                }));

            Assert.Contains("today()", exception.Message);
            Assert.Contains("not supported in filter or subscription expressions", exception.Message);
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(DatabaseMode = RavenDatabaseMode.All)]
    public async Task Today_ShouldReturnNotModified(Options options)
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

                // today() resolves to the same ticks within the same day, so the ETag should match
                Assert.Equal(HttpStatusCode.NotModified, command2.StatusCode);
            }
        }
    }

    // --- Offset tests ---

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WithPositiveOffset_ReturnsCorrectDocuments(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(5) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(20) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                // now('+7d') adds 7 days, floors to day boundary
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt <= now('+7d')")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WithNegativeOffset_ReturnsCorrectDocuments(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-10) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(1) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                // now('-3d') subtracts 3 days, floors to day boundary
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt <= now('-3d')")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(1, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WithParameterOffset_ReturnsCorrectDocuments(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(5) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(20) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt <= now($offset)")
                    .AddParameter("offset", "+7d")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WithOffset_DocumentQuery_ReturnsCorrectDocuments(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(5) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(20) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced.DocumentQuery<Employee>()
                    .WhereLessThanOrEqual("HiredAt", RavenDocumentQuery.Now("+7d"))
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WithOffset_Linq_ReturnsCorrectDocuments(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(5) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(20) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Query<Employee>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.HiredAt <= RavenQuery.Now("+7d"))
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WithMultiUnitOffset_ReturnsCorrectDocuments(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(1).AddHours(3) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(2) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                // now('+1d5h') adds 1 day and 5 hours, floors to hour boundary
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt <= now('+1d5h')")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WithoutOffset_StillWorks(Options options)
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
                    .RawQuery<Employee>("from Employees where HiredAt <= now()")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(1, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WithInvalidOffset_Throws(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var ex = Assert.ThrowsAny<RavenException>(() =>
                {
                    session.Advanced
                        .RawQuery<Employee>("from Employees where HiredAt <= now('invalid')")
                        .WaitForNonStaleResults()
                        .ToList();
                });
                Assert.Contains("Invalid offset format", ex.Message);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WithOffset_IsCaseInsensitive(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(5) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(20) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt <= NOW('+7d')")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Today_WithOffset_IsRejected(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var ex = Assert.ThrowsAny<RavenException>(() =>
                {
                    session.Advanced
                        .RawQuery<Employee>("from Employees where HiredAt < today('+1d')")
                        .WaitForNonStaleResults()
                        .ToList();
                });
                Assert.Contains("today() does not accept arguments", ex.Message);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WithReadableUnitNames_ReturnsCorrectDocuments(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(-1) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(5) });
                session.Store(new Employee { HiredAt = DateTime.UtcNow.AddDays(20) });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var employees = session.Advanced
                    .RawQuery<Employee>("from Employees where HiredAt <= now('+7 days')")
                    .WaitForNonStaleResults()
                    .ToList();

                Assert.Equal(2, employees.Count);
            }
        }
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
    public void Now_WithEmptyOffset_Throws(Options options)
    {
        using (var store = GetDocumentStore(options))
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Employee { HiredAt = DateTime.UtcNow });
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var ex = Assert.ThrowsAny<RavenException>(() =>
                {
                    session.Advanced
                        .RawQuery<Employee>("from Employees where HiredAt <= now('')")
                        .WaitForNonStaleResults()
                        .ToList();
                });
                Assert.Contains("offset argument must be a non-empty string", ex.Message);
            }
        }
    }
}
