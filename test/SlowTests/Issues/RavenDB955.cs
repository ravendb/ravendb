using System;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB955 : RavenTestBase
    {
        public RavenDB955(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanQueryWithNullComparison()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new WithNullableField { TheNullableField = 1 });
                    s.Store(new WithNullableField { TheNullableField = null });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {

                    Assert.Equal(1, s.Query<WithNullableField>().Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue)).Count(x => x.TheNullableField == null));
                    Assert.Equal(1, s.Query<WithNullableField>().Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue)).Count(x => x.TheNullableField != null));
                }
            }

        }

        [Fact]
        public void CanQueryWithHasValue()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new WithNullableField { TheNullableField = 1 });
                    s.Store(new WithNullableField { TheNullableField = null });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.Equal(1, s.Query<WithNullableField>().Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue)).Count(x => !x.TheNullableField.HasValue));
                    Assert.Equal(1, s.Query<WithNullableField>().Customize(x => x.WaitForNonStaleResults(TimeSpan.MaxValue)).Count(x => x.TheNullableField.HasValue));
                }
            }

        }

        private class WithNullableField
        {
            public int? TheNullableField { get; set; }
        }
    }
}
