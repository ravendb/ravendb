using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22317 : RavenTestBase
{
    public RavenDB_22317(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes)]
    public void TestIndexWithGroupByUsingMethod()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenSession())
            {
                var foo1 = new Foo() { Day = DayOfWeek.Monday, Value = "Monday" };
                var foo2 = new Foo() { Day = DayOfWeek.Friday, Value = "Friday" };
                var foo3 = new Foo() { Day = DayOfWeek.Monday, Value = "Monday again" };
                
                session.Store(foo1);
                session.Store(foo2);
                session.Store(foo3);
                
                session.SaveChanges();
                
                var index = new FooIndex();
            
                index.Execute(store);
            
                Indexes.WaitForIndexing(store);
                
                var mondayResult = session.Query<FooIndex.Result, FooIndex>().Where(x => x.Day == DayOfWeek.Monday).ToList();
                
                Assert.Equal(1, mondayResult.Count);
                
                Assert.Equal(2, mondayResult.First().Values.Length);
                
                var fridayResult = session.Query<FooIndex.Result, FooIndex>().Where(x => x.Day == DayOfWeek.Friday).ToList();
                
                Assert.Equal(1, fridayResult.Count);
                
                Assert.Equal(1, fridayResult.First().Values.Length);
            }
        }
    }
    
    private class Foo
    {
        public string Id { get; set; }

        public DayOfWeek Day { get; set; }

        public string Value { get; set; }
    }

    private class FooIndex : AbstractIndexCreationTask<Foo, FooIndex.Result>
    {
        public FooIndex()
        {
            Map = foos =>
                from f in foos
                select new Result { Day = f.Day, Values = new[] { f.Value }, };

            Reduce = results =>
                from result in results
                group result by new { Day = IndexHelper.EnsureEnum(result.Day) }
                into g
                select new Result { Day = g.Key.Day, Values = g.SelectMany(x => x.Values).ToArray() };

            AdditionalSources = new()
            {
                {
                    "RavenEnumReduce", @"
                    public static class IndexHelper 
                    {
                        public static string[] DaysOfWeek = new string[] { ""Sunday"", ""Monday"", ""Tuesday"", ""Wednesday"", ""Thursday"", ""Friday"", ""Saturday"" };
                        public static string EnsureEnum(object value)
                        {
                            if (value is string s)
                            {
                                return s;
                            }

                            if (value is Sparrow.Json.LazyStringValue lsv)
                            {
                                return lsv.ToString();
                            }

                            return DaysOfWeek[(int)value];
                        }
                    }
                    "
                }
            };
        }

        public class Result
        {
            public DayOfWeek Day { get; set; }
            public string[] Values { get; set; }
        }

        private static class IndexHelper
        {
            public static DayOfWeek EnsureEnum(object value) => throw new Exception();
        }
    }
}
