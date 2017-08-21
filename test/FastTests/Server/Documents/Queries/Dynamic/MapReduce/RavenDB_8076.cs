using Raven.Client.Documents.Queries;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace FastTests.Server.Documents.Queries.Dynamic.MapReduce
{
    public class RavenDB_8076 : RavenTestBase
    {
        [Fact]
        public void Can_sort_by_aggregation_function_in_dynamic_group_by_query()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Company
                    {
                        Address1 = "UK",
                        AccountsReceivable = 1
                    });

                    s.Store(new Company
                    {
                        Address1 = "UK",
                        AccountsReceivable = 1
                    });

                    s.Store(new Company
                    {
                        Address1 = "USA",
                        AccountsReceivable = 1
                    });

                    s.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    foreach (var q in new(string Query, string Field)[]
                    {
                        (@"select count()
                        from Companies
                        group by Address1 order by count()", "Count"),

                        (@"select count() as MyCount
                        from Companies
                        group by Address1 order by count() as long", "MyCount"),

                        (@"select count() as Count
                        from Companies
                        group by Address1 order by Count as long", "Count"),
                    })
                    {
                        // asc

                        var results = commands.Query(new IndexQuery { Query = q.Query }).Results;

                        Assert.Equal(2, results.Length);

                        var item = (BlittableJsonReaderObject)results[0];
                        Assert.True(item.TryGet(q.Field, out long count));
                        Assert.Equal(1, count);

                        item = (BlittableJsonReaderObject)results[1];
                        Assert.True(item.TryGet(q.Field, out count));
                        Assert.Equal(2, count);

                        // desc

                        results = commands.Query(new IndexQuery { Query = q.Query + " desc" }).Results;

                        item = (BlittableJsonReaderObject)results[0];
                        Assert.True(item.TryGet(q.Field, out count));
                        Assert.Equal(2, count);

                        item = (BlittableJsonReaderObject)results[1];
                        Assert.True(item.TryGet(q.Field, out count));
                        Assert.Equal(1, count);
                    }
                }

                using (var commands = store.Commands())
                {
                    foreach (var q in new (string Query, string Field)[]
                    {
                        (@"select sum(AccountsReceivable)
                        from Companies
                        group by Address1 order by sum(AccountsReceivable)", "AccountsReceivable"),

                        (@"select sum(AccountsReceivable) as Sum
                        from Companies
                        group by Address1 order by sum(AccountsReceivable) as double", "Sum"),

                        (@"select sum(AccountsReceivable) as Sum
                        from Companies
                        group by Address1 order by Sum", "Sum")
                    })
                    {
                        // asc

                        var results = commands.Query(new IndexQuery { Query = q.Query }).Results;

                        Assert.Equal(2, results.Length);

                        var item = (BlittableJsonReaderObject)results[0];
                        Assert.True(item.TryGet(q.Field, out decimal sum));
                        Assert.Equal(1, sum);

                        item = (BlittableJsonReaderObject)results[1];
                        Assert.True(item.TryGet(q.Field, out sum));
                        Assert.Equal(2, sum);

                        // desc

                        results = commands.Query(new IndexQuery { Query = q.Query + " desc" }).Results;

                        item = (BlittableJsonReaderObject)results[0];
                        Assert.True(item.TryGet(q.Field, out sum));
                        Assert.Equal(2, sum);

                        item = (BlittableJsonReaderObject)results[1];
                        Assert.True(item.TryGet(q.Field, out sum));
                        Assert.Equal(1, sum);
                    }
                }
            }
        }

        [Fact]
        public void Filter_by_aggregation_function_in_dynamic_group_by_query()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Company
                    {
                        Address1 = "UK",
                        AccountsReceivable = 1
                    });

                    s.Store(new Company
                    {
                        Address1 = "UK",
                        AccountsReceivable = 1
                    });

                    s.Store(new Company
                    {
                        Address1 = "USA",
                        AccountsReceivable = 1
                    });

                    s.SaveChanges();
                }

                using (var commands = store.Commands())
                {
                    foreach (var q in new(string Query, string Field)[]
                    {
                        (@"select count()
                        from Companies
                        group by Address1 where count() >= 2", "Count"),

                        (@"select count() as MyCount
                        from Companies
                        group by Address1 where count() in (2, 3)", "MyCount"),

                        (@"select count() as Count
                        from Companies
                        group by Address1 where Count >= 2", "Count")
                    })
                    {
                        var results = commands.Query(new IndexQuery { Query = q.Query }).Results;

                        Assert.Equal(1, results.Length);

                        var item = (BlittableJsonReaderObject)results[0];
                        Assert.True(item.TryGet(q.Field, out long count));
                        Assert.Equal(2, count);
                    }
                }

                using (var commands = store.Commands())
                {
                    foreach (var q in new(string Query, string Field)[]
                    {
                        (@"select sum(AccountsReceivable)
                        from Companies
                        group by Address1 where sum(AccountsReceivable) > 1", "AccountsReceivable"),

                        (@"select sum(AccountsReceivable) as Sum
                        from Companies
                        group by Address1 where sum(AccountsReceivable) between 2 and 5", "Sum"),

                        (@"select sum(AccountsReceivable) as Sum
                        from Companies
                        group by Address1 where Sum > 1", "Sum")
                    })
                    {
                        var results = commands.Query(new IndexQuery { Query = q.Query }).Results;

                        Assert.Equal(1, results.Length);

                        var item = (BlittableJsonReaderObject)results[0];
                        Assert.True(item.TryGet(q.Field, out decimal sum));
                        Assert.Equal(2, sum);
                    }
                }
            }
        }
    }
}
