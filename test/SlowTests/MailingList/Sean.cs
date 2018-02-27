// -----------------------------------------------------------------------
//  <copyright file="Sean.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using FastTests;
using Raven.Client.Documents.Queries.Facets;
using Sparrow.Extensions;
using Xunit;

namespace SlowTests.MailingList
{
    public class Sean : RavenTestBase
    {
        private class Thread
        {
            public DateTime? CreationDate { get; set; }
        }

        [Fact]
        public void CanUseNullablesForFacets()
        {
            RangeFacet facet = new RangeFacet<Thread>
            {
                Ranges =
                {
                    t => t.CreationDate.Value < new DateTime(2012, 1, 1),
                }
            };

            Assert.Equal(@"CreationDate < $p0", facet.Ranges[0]);
            var parameter = ((DateTime)facet.FacetParameters["p0"]).GetDefaultRavenFormat();
            Assert.Equal("2012-01-01T00:00:00.0000000", parameter);
        }
    }
}
