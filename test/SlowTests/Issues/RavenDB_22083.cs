using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
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

        private class IndexWithOrderedReduce : AbstractIndexCreationTask<Candle, AggregateCandle>
        {
            public IndexWithOrderedReduce()
            {
                Map = candles => from candle in candles
                    select new AggregateCandle
                    {
                        Ref = $"{candle.Time / 60000}",
                        Volume = candle.Volume,
                        Timeframe = "1m"
                    };

                Reduce = results => results
                    .GroupBy(x => new { x.Timeframe, x.Ref })
                    .Select(g => new AggregateCandle
                    {
                        Ref = g.Key.Ref,
                        Volume = g.Sum(x => x.Volume),
                        Timeframe = g.Key.Timeframe
                    })
                    .OrderBy(x => x.Ref);
            }
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

        [RavenFact(RavenTestCategory.Indexes)]
        public void Index_With_Ordered_Reduce_Deploys_And_Indexes()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new IndexWithOrderedReduce());

                using (var session = store.OpenSession())
                {
                    session.Store(new Candle { Volume = 1, Time = 60000 });
                    session.Store(new Candle { Volume = 2, Time = 60000 });
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<AggregateCandle, IndexWithOrderedReduce>().ToList();

                    Assert.Equal(1, results.Count);
                    Assert.Equal(3, results[0].Volume);
                }
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Index_With_Constant_Array_As_Outer_Source_Is_Rejected_At_Compilation()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.ExecuteIndex(new IndexWithConstantArrayAsOuterSource()));

                Assert.Equal(nameof(IndexDefinition.Maps), e.IndexDefinitionProperty);
                Assert.Contains("a C# map must start its enumeration from one of these sources", e.Message);
                Assert.Contains("SelectMany(x => docs.Candles,", e.ProblematicText);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Index_With_Parameter_Name_Prefixing_Outer_Source_Is_Rejected_At_Compilation()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.ExecuteIndex(new IndexWithParameterNamePrefixingOuterSource()));

                Assert.Equal(nameof(IndexDefinition.Maps), e.IndexDefinitionProperty);
                Assert.Contains("a C# map must start its enumeration from one of these sources", e.Message);
                Assert.Contains("SelectMany(t => docs.Candles,", e.ProblematicText);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void MultiMap_Index_With_Constant_Array_As_Outer_Source_Is_Rejected_At_Compilation()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.ExecuteIndex(new MultiMapIndexWithConstantArrayAsOuterSource()));

                Assert.Equal(nameof(IndexDefinition.Maps), e.IndexDefinitionProperty);
                Assert.Contains("a C# map must start its enumeration from one of these sources", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Index_Definition_Sent_As_Text_With_Constant_Array_As_Outer_Source_Throws_IndexCompilationException()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "CandlesByTimeframeFromText",
                    Maps =
                    {
                        @"from x in new[] { new { Timeframe = ""1m"", Interval = 60000L }, new { Timeframe = ""5m"", Interval = 300000L } }
                           from candle in docs.Candles
                           select new { Ref = $""{candle.Time / x.Interval}"", Volume = candle.Volume, Timeframe = x.Timeframe }"
                    }
                })));

                Assert.Contains("Failed to compile index 'CandlesByTimeframeFromText'", e.Message);
                Assert.Contains("must start its enumeration from 'docs'", e.Message);
                Assert.Equal(nameof(IndexDefinition.Maps), e.IndexDefinitionProperty);
            }
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void Index_Definition_Sent_As_Text_Not_Rooted_In_Documents_Source_Throws_IndexCompilationException()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "CandlesFromMethodSyntaxText",
                    Maps = { "new[] { 60000L, 300000L }.SelectMany(t => docs.Candles, (t, candle) => new { Ref = candle.Time / t, Volume = candle.Volume })" }
                })));

                Assert.Equal(nameof(IndexDefinition.Maps), e.IndexDefinitionProperty);
                Assert.Contains("compiled as JavaScript", e.Message);
                Assert.Contains("a C# map must start its enumeration from one of these sources", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
        public void JavaScript_Index_With_Invalid_Map_Throws_IndexCompilationException()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "BrokenJsMap",
                    Maps = { "map('Candles', function (c) { return { Volume: c.Volume }; )" }
                })));

                Assert.Equal(nameof(IndexDefinition.Maps), e.IndexDefinitionProperty);
            }
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
        public void JavaScript_Index_With_Invalid_Reduce_Throws_IndexCompilationException()
        {
            using (var store = GetDocumentStore())
            {
                var e = Assert.Throws<IndexCompilationException>(() => store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "BrokenJsReduce",
                    Maps = { "map('Candles', function (c) { return { Volume: c.Volume }; })" },
                    Reduce = "groupBy(x => x.Volume).aggregate(g => { return { Volume: g.values.reduce((a, b) => a + b.Volume, 0) }; )"
                })));

                Assert.Equal(nameof(IndexDefinition.Reduce), e.IndexDefinitionProperty);
            }
        }
    }
}
