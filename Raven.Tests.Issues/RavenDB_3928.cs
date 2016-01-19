// -----------------------------------------------------------------------
//  <copyright file="RavenDB_3928.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto.Faceted;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3928 : RavenTest
    {
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
            var expected1 = Facet<Person2>.Parse(x => x.Age1 < 15);
            var expected2 = Facet<Person2>.Parse(x => x.Age1 >= 15 && x.Age1 < 25);

            var actual1 = Facet<Person>.Parse(x => x.Age1 < 15);
            var actual2 = Facet<Person>.Parse(x => x.Age1 >= 15 && x.Age1 < 25);

            Assert.Equal(expected1, actual1);
            Assert.Equal(expected2, actual2);

            expected1 = Facet<Person2>.Parse(x => x.Age2 < 15);
            expected2 = Facet<Person2>.Parse(x => x.Age2 >= 15 && x.Age2 < 25);

            actual1 = Facet<Person>.Parse(x => x.Age2 < 15);
            actual2 = Facet<Person>.Parse(x => x.Age2 >= 15 && x.Age2 < 25);

            Assert.Equal(expected1, actual1);
            Assert.Equal(expected2, actual2);

            expected1 = Facet<Person2>.Parse(x => x.Age3 < 15);
            expected2 = Facet<Person2>.Parse(x => x.Age3 >= 15 && x.Age3 < 25);

            actual1 = Facet<Person>.Parse(x => x.Age3 < 15);
            actual2 = Facet<Person>.Parse(x => x.Age3 >= 15 && x.Age3 < 25);

            Assert.Equal(expected1, actual1);
            Assert.Equal(expected2, actual2);
        }
    }
}