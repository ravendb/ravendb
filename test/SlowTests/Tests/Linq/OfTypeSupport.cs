// -----------------------------------------------------------------------
//  <copyright file="OfTypeSupport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class OfTypeSupport : RavenTestBase
    {
        [Fact]
        public void OfTypeWillBeConvertedToWhere()
        {
            using (var store = GetDocumentStore())
            {
                new TagSummaryIndex().Execute(store);
            }
        }

        private class Foo
        {
            public string Tag { get; set; }
            public List<Bar> Bars { get; set; }
        }

        private abstract class Bar
        {
            public int Weight { get; set; }
        }

        private class IronBar : Bar { }

        private class ChocolateBar : Bar { }

        private class TagSummary
        {
            public string Tag { get; set; }
            public int Count { get; set; }
            public int TotalChocolateBarWeight { get; set; }
            public int TotalIronBarWeight { get; set; }
        }

        private class TagSummaryIndex : AbstractIndexCreationTask<Foo, TagSummary>
        {
            public TagSummaryIndex()
            {
                Map = foos => from f in foos
                              select new
                              {
                                  Tag = f.Tag,
                                  Count = 1,
                                  TotalChocolateBarWeight = f.Bars.OfType<ChocolateBar>().Sum(x => x.Weight),
                                  TotalIronBarWeight = f.Bars.OfType<IronBar>().Sum(x => x.Weight)
                              };

                Reduce = results => from r in results
                                    group r by r.Tag into g
                                    select new
                                    {
                                        Tag = g.Key,
                                        Count = g.Sum(x => x.Count),
                                        TotalChocolateBarWeight = g.Sum(x => x.TotalChocolateBarWeight),
                                        TotalIronBarWeight = g.Sum(x => x.TotalIronBarWeight)
                                    };
            }
        }
    }
}
