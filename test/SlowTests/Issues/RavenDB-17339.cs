using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17339 : RavenTestBase
    {
        public RavenDB_17339(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanRawProjectOnNestedTypes()
        {
            using var store = GetDocumentStore();
            {
                using var session = store.OpenSession();
                session.Store(new MyEntity() { Value = 123, Result = new() { RawValue = 1234 } });
                session.SaveChanges();
            }
            {
                using var session = store.OpenSession();
                var firstQuery = session.Query<MyEntity>()
                    .Customize(q => q.WaitForNonStaleResults())
                    .Select(e => new Result { RawValue = RavenQuery.Raw<int>("e.Value") })
                    .ToList();
                Assert.Equal(1, firstQuery.Count);
                Assert.Equal(123, firstQuery[0].RawValue);

                var secondQuery = session.Query<MyEntity>()
                    .Customize(q => q.WaitForNonStaleResults())
                    .Select(e => new { Nested = new Result { RawValue = RavenQuery.Raw<int>("e.Value") } })
                    .ToList();
                Assert.Equal(1, secondQuery.Count);
                Assert.Equal(123, secondQuery[0].Nested.RawValue);

                var thirdQuery = session.Query<MyEntity>()
                    .Customize(q => q.WaitForNonStaleResults())
                    .Select(e => new { Nested = new Result { RawValue = e.Value } })
                    .ToList();
                Assert.Equal(1, thirdQuery.Count);
                Assert.Equal(123, thirdQuery[0].Nested.RawValue);

                var q5 = session.Query<MyEntity>()
                    .Select(e => new { Foo = e.Value, Nested = new Result { RawValue = RavenQuery.Raw<int>("e.Value") } })
                    .ToList();
                Assert.Equal(1, q5.Count);
                Assert.Equal(123, q5[0].Nested.RawValue);

                var q6 = session.Query<MyEntity>()
                    .Select(e => new { Nested = new { RawValue = RavenQuery.Raw<int>("e.Value") } })
                    .ToList();
                Assert.Equal(1, q6.Count);
                Assert.Equal(123, q6[0].Nested.RawValue);

                var q7 = session.Query<MyEntity>()
                    .Select(e => new
                    {
                        Nested = new { RawValue = new Result { RawValue = RavenQuery.Raw<int>("e.Value") } },
                        Test = new Result { RawValue = RavenQuery.Raw<int>("e.Value") }
                    })
                    .ToList();
                Assert.Equal(1, q7.Count);
                Assert.Equal(123, q7[0].Nested.RawValue.RawValue);

                var q8 = session.Query<MyEntity>()
                    .Select(e => new { Abc = 1, Cba = 2, RawValue = RavenQuery.Raw<int>("e.Value"), Nest = new Result { RawValue = RavenQuery.Raw<int>("e.Value") } })
                    .ToList();
                Assert.Equal(1, q8[0].Abc);
                Assert.Equal(2, q8[0].Cba);
                Assert.Equal(123, q8[0].RawValue);
                Assert.Equal(123, q8[0].Nest.RawValue);

                var q9 = session.Query<MyEntity>()
                    .Select(
                        e => new MyEntity() { Id = "test", Result = new Result { RawValue = RavenQuery.Raw<int>("e.Value") }, Value = RavenQuery.Raw<int>("e.Value") })
                    .ToList();
                Assert.Equal("test", q9[0].Id);
                Assert.Equal(123, q9[0].Result.RawValue);
                Assert.Equal(123, q9[0].Value);
            }
        }

        private class MyEntity
        {
            public string Id { get; set; }
            public int Value { get; set; }
            public Result Result { get; set; }
        }

        private class Result
        {
            public int RawValue { get; set; }
        }
    }
}
