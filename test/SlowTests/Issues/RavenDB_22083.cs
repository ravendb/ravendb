using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_22083 : RavenTestBase
    {
        public RavenDB_22083(ITestOutputHelper output) : base(output)
        {
        }

        private class Candle
        {
            public string Id { get; set; }
            public float[] Ohlc { get; set; }
            public float Volume { get; set; }
            public long Time { get; set; }
        }

        private class AggregateCandle
        {
            public string Ref { get; set; }
            public float Volume { get; set; }
            public string Timeframe { get; set; }
        }

        private class IndexWithConstantArrayAsOuterSource : AbstractIndexCreationTask<Candle, AggregateCandle>
        {
            public IndexWithConstantArrayAsOuterSource()
            {
                Map = candles => from x in new[]
                    {
                        new Tuple<long, string>(60000, "1m"),
                        new Tuple<long, string>(60000 * 5, "5m")
                    }
                    from candle in candles
                    select new AggregateCandle
                    {
                        Ref = $"{candle.Time / x.Item1}",
                        Volume = candle.Volume,
                        Timeframe = x.Item2
                    };

                Reduce = results => from result in results
                    group result by new { result.Timeframe, result.Ref }
                    into g
                    select new AggregateCandle
                    {
                        Ref = g.Key.Ref,
                        Volume = g.Sum(x => x.Volume),
                        Timeframe = g.Key.Timeframe
                    };
            }
        }

        private class IndexWithConstantArrayAsInnerSource : AbstractIndexCreationTask<Candle, AggregateCandle>
        {
            public IndexWithConstantArrayAsInnerSource()
            {
                Map = candles => from candle in candles
                    from x in new[]
                    {
                        new Tuple<long, string>(60000, "1m"),
                        new Tuple<long, string>(60000 * 5, "5m")
                    }
                    select new AggregateCandle
                    {
                        Ref = $"{candle.Time / x.Item1}",
                        Volume = candle.Volume,
                        Timeframe = x.Item2
                    };

                Reduce = results => from result in results
                    group result by new { result.Timeframe, result.Ref }
                    into g
                    select new AggregateCandle
                    {
                        Ref = g.Key.Ref,
                        Volume = g.Sum(x => x.Volume),
                        Timeframe = g.Key.Timeframe
                    };
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Index_With_Constant_Array_As_Outer_Source_Throws_Descriptive_Exception()
        {
            var e = Assert.Throws<IndexCompilationException>(() => new IndexWithConstantArrayAsOuterSource().CreateIndexDefinition());

            Assert.Contains("must start its enumeration from the 'candles' parameter", e.InnerException.Message);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Index_With_Constant_Array_As_Inner_Source_Deploys_And_Indexes()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IndexWithConstantArrayAsInnerSource());

                using (var session = store.OpenSession())
                {
                    session.Store(new Candle { Volume = 1, Time = 60000 });
                    session.Store(new Candle { Volume = 2, Time = 60000 });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<AggregateCandle, IndexWithConstantArrayAsInnerSource>()
                        .Where(x => x.Timeframe == "1m")
                        .ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(3, results[0].Volume);
                }
            }
        }
    }
}
