using System;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions.Documents.Compilation;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
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

        private class IndexWithParameterNamePrefixingOuterSource : AbstractIndexCreationTask<Candle, AggregateCandle>
        {
            public IndexWithParameterNamePrefixingOuterSource()
            {
                Map = n => from t in new[] { 60000L, 300000L }
                    from candle in n
                    select new AggregateCandle
                    {
                        Ref = $"{candle.Time / t}",
                        Volume = candle.Volume,
                        Timeframe = "1m"
                    };
            }
        }

        private class MultiMapIndexWithConstantArrayAsOuterSource : AbstractMultiMapIndexCreationTask<AggregateCandle>
        {
            public MultiMapIndexWithConstantArrayAsOuterSource()
            {
                AddMap<Candle>(candles => from x in new[]
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
                    });
            }
        }

        private class Animal
        {
            public string Name { get; set; }
        }

        private class Dog : Animal
        {
        }

        private class Cat : Animal
        {
        }

        private class MultiMapIndexWithAddMapForAll : AbstractMultiMapIndexCreationTask
        {
            public MultiMapIndexWithAddMapForAll()
            {
                AddMapForAll<Animal>(animals => from animal in animals select new { animal.Name });
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Index_With_Constant_Array_As_Outer_Source_Throws_Descriptive_Exception()
        {
            var e = Assert.Throws<IndexCompilationException>(() => new IndexWithConstantArrayAsOuterSource().CreateIndexDefinition());

            Assert.Contains("must start its enumeration from the 'candles' parameter", e.InnerException.Message);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Index_With_Parameter_Name_Prefixing_Outer_Source_Throws_Descriptive_Exception()
        {
            var e = Assert.Throws<IndexCompilationException>(() => new IndexWithParameterNamePrefixingOuterSource().CreateIndexDefinition());

            Assert.Contains("must start its enumeration from the 'n' parameter", e.InnerException.Message);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void MultiMap_Index_With_Constant_Array_As_Outer_Source_Throws_Descriptive_Exception()
        {
            var e = Assert.Throws<IndexCompilationException>(() => new MultiMapIndexWithConstantArrayAsOuterSource().CreateIndexDefinition());

            Assert.Contains("Failed to create index", e.Message);
            Assert.Contains("must start its enumeration from the 'candles' parameter", e.InnerException.Message);
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void MultiMap_Index_With_AddMapForAll_Creates_Definition()
        {
            var definition = new MultiMapIndexWithAddMapForAll().CreateIndexDefinition();

            Assert.Equal(3, definition.Maps.Count);
        }
    }
}
