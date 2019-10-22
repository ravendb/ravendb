using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using FastTests.Utils;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs
{
    public class CanPassTypesProperlyToAggregation : RavenTestBase
    {
        public CanPassTypesProperlyToAggregation(ITestOutputHelper output) : base(output)
        {
        }

        private class Coin
        {
            public int Denomination { get; set; }
            public double Cost { get; set; }
        }

        [Fact]
        public void WillGenerateDecimalCast()
        {
            Expression<Func<IEnumerable<Coin>, IEnumerable<object>>> query = x => from y in x
                                                                                  group y by y.Denomination
                                                                                  into g
                                                                                  select new
                                                                                  {
                                                                                      Denomination = g.Key,
                                                                                      Cost = g.Sum(z => z.Cost)
                                                                                  };


            var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Coin, Coin>(query, new DocumentConventions(), "docs", false);

            Assert.Equal(LinuxTestUtils.Dos2Unix(@"docs.GroupBy(y => y.Denomination).Select(g => new {
    Denomination = g.Key,
    Cost = Enumerable.Sum(g, z => ((double) z.Cost))
})"), code);
        }

        [Fact]
        public void WillProperlyCompileWhenUsingToString()
        {
            Expression<Func<IEnumerable<Coin>, IEnumerable<object>>> query = x => from y in x
                                                                                  group y by y.Denomination
                                                                                  into g
                                                                                  select
                                                                                      new
                                                                                      {
                                                                                          Denomination = g.Key,
                                                                                          Cost = g.First().Cost.ToString()
                                                                                      };


            var code = IndexDefinitionHelper.PruneToFailureLinqQueryAsStringToWorkableCode<Coin, Coin>(query, new DocumentConventions(), "docs", false);

            Assert.Equal(LinuxTestUtils.Dos2Unix(@"docs.GroupBy(y => y.Denomination).Select(g => new {
    Denomination = g.Key,
    Cost = (DynamicEnumerable.FirstOrDefault(g)).Cost.ToString()
})"), code);
        }
    }
}
