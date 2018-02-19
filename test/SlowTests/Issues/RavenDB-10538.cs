using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Queries.Facets;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10538 : RavenTestBase
    {
        private class Foo
        {
            public DateTime? DateIn { get; set; }
        }

        [Fact]
        public void FacetWithNullableDateTime()
        {
            var now = DateTime.Now;
            var facets = new List<RangeFacet>
            {
                new RangeFacet<Foo>
                {
                    Ranges = { c => c.DateIn < now - TimeSpan.FromDays(365)}
                }
            };
        }
    }
}
