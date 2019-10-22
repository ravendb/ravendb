// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3928.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Client.Documents.Queries.Facets;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3928 : RavenTestBase
    {
        public RavenDB_3928(ITestOutputHelper output) : base(output)
        {
        }

        private class Person
        {
            public int? Age1 { get; set; }

            public decimal? Age2 { get; set; }

            public double? Age3 { get; set; }
        }

        private class Person2
        {
            public int Age1 { get; set; }

            public decimal Age2 { get; set; }

            public double Age3 { get; set; }
        }

        [Fact]
        public void FacetShouldWorkWithNullableExpressions()
        {
            var expected1 = RangeFacet<Person2>.Parse(x => x.Age1 < 15);
            var expected2 = RangeFacet<Person2>.Parse(x => x.Age1 >= 15 && x.Age1 < 25);

            var actual1 = RangeFacet<Person>.Parse(x => x.Age1 < 15);
            var actual2 = RangeFacet<Person>.Parse(x => x.Age1 >= 15 && x.Age1 < 25);

            Assert.Equal(expected1, actual1);
            Assert.Equal(expected2, actual2);

            expected1 = RangeFacet<Person2>.Parse(x => x.Age2 < 15);
            expected2 = RangeFacet<Person2>.Parse(x => x.Age2 >= 15 && x.Age2 < 25);

            actual1 = RangeFacet<Person>.Parse(x => x.Age2 < 15);
            actual2 = RangeFacet<Person>.Parse(x => x.Age2 >= 15 && x.Age2 < 25);

            Assert.Equal(expected1, actual1);
            Assert.Equal(expected2, actual2);

            expected1 = RangeFacet<Person2>.Parse(x => x.Age3 < 15);
            expected2 = RangeFacet<Person2>.Parse(x => x.Age3 >= 15 && x.Age3 < 25);

            actual1 = RangeFacet<Person>.Parse(x => x.Age3 < 15);
            actual2 = RangeFacet<Person>.Parse(x => x.Age3 >= 15 && x.Age3 < 25);

            Assert.Equal(expected1, actual1);
            Assert.Equal(expected2, actual2);
        }
    }
}
