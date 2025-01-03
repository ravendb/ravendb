using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_23464(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDeployCoraxCsharpMapReduceIndexWithVectorField(Options options)
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        new MapReduceIndexCorax().Execute(store);
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDeployCoraxCsharpMapIndexWithVectorField(Options options)
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        new CsharpIndexBaseCorax().Execute(store);
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDeployCoraxJsMapIndexWithVectorField(Options options)
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        new JsIndexCorax().Execute(store);
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDeployCoraxCsharpCounterIndexWithVectorField(Options options)
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        new CounterIndexCorax().Execute(store);
    }
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDeployCoraxCsharpTimeSeriesIndexWithVectorField(Options options)
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        new TimeSeriesIndexCorax().Execute(store);
    }
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterIndexBase() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterTestBase<CounterIndexBase>();
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterIndexExplicitLucene(Options options) =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterTestBase<CounterIndexLucene>(options);
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesIndexBase() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesTestBase<TimeSeriesIndexBase>();
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesIndexExplicitLucene() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesTestBase<TimeSeriesIndexLucene>();
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpMapIndexBase() => ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<CsharpIndexBase>();
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpMapIndexExplicitLucene(Options options) => ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<CsharpIndexBaseLucene>(options);
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxJavaScriptBase() => ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<JsIndexBase>();
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxJavaScriptLucene(Options options) => ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<JsIndexLucene>(options);
    
    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxMapReduceBase() => ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<MapReduceIndexBase>();
    
    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxMapReduceLucene(Options options) => ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<MapReduceIndexLucene>(options);
    
    private void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<TIndex>(Options options = null) where TIndex : AbstractIndexCreationTask, new()
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        AssertExceptionOfVectorFieldInIndex(() => new TIndex().Execute(store));
    }
    
    private void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterTestBase<TIndex>(Options options = null)
        where TIndex : AbstractCountersIndexCreationTask, new()
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        AssertExceptionOfVectorFieldInIndex(() => new TIndex().Execute(store));
    }
    
    private void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesTestBase<TIndex>(Options options = null)
        where TIndex : AbstractTimeSeriesIndexCreationTask, new()
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        AssertExceptionOfVectorFieldInIndex(() => new TIndex().Execute(store));
    }

    private static void AssertExceptionOfVectorFieldInIndex(Action indexDeployment)
    {
        var ravenException = Assert.Throws<RavenException>(indexDeployment);
        Assert.IsType<NotSupportedException>(ravenException.InnerException);
        Assert.Contains("Vector fields are supported only by the Corax search engine. This deployment requested 'Lucene' search engine. Read more at https://ravendb.net/l/Y4B762/7.0", ravenException.InnerException.Message);
    }

    private class Dto
    {
        public string Text { get; set; }
        public string Id { get; set; }
    }

    private class CsharpIndexBaseCorax : CsharpIndexBase
    {
        public CsharpIndexBaseCorax() : base()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
    
    private class CsharpIndexBaseLucene : CsharpIndexBase
    {
        public CsharpIndexBaseLucene() : base()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }
    
    private class CsharpIndexBase : AbstractIndexCreationTask<Dto>
    {
        public CsharpIndexBase()
        {
            Map = dtos => dtos.Select(d => new { Vector = CreateVector(d.Text) });
        }
    }

    private class JsIndexCorax : JsIndexBase
    {
        public JsIndexCorax()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
    
    private class JsIndexLucene : JsIndexBase
    {
        public JsIndexLucene()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }
    
    private class JsIndexBase : AbstractJavaScriptIndexCreationTask
    {
        public JsIndexBase()
        {
            Maps = [@$"map('Dtos', function (e) {{
    return {{ 
        Name: e.Name,
        Vector: createVector(e.Text)
    }};
}})"];
        }
    }

    private class CounterIndexCorax : CounterIndexBase
    {
        public CounterIndexCorax()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
    
    private class CounterIndexLucene : CounterIndexBase
    {
        public CounterIndexLucene()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }
    
    private class CounterIndexBase : AbstractCountersIndexCreationTask<Company>
    {
        public CounterIndexBase()
        {
            AddMapForAll(counters => from counter in counters
                select new
                {
                    HeartBeat = counter.Value,
                    Name = counter.Name,
                    User = counter.DocumentId,
                    Vector = CreateVector(counter.Name)
                });
        }
    }

    private class TimeSeriesIndexCorax : TimeSeriesIndexBase
    {
        public TimeSeriesIndexCorax()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }
    
    private class TimeSeriesIndexLucene : TimeSeriesIndexBase
    {
        public TimeSeriesIndexLucene()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }
    
    private class TimeSeriesIndexBase : AbstractTimeSeriesIndexCreationTask<Company>
    {
        public TimeSeriesIndexBase()
        {
            AddMap(
                "HeartRate",
                timeSeries => from ts in timeSeries
                    from entry in ts.Entries
                    select new
                    {
                        HeartBeat = entry.Values[0],
                        entry.Timestamp.Date,
                        User = ts.DocumentId,
                        Vector = CreateVector(ts.DocumentId)
                    });
        }
    }
    
    private class MapReduceIndexLucene : MapReduceIndexBase
    {
        public MapReduceIndexLucene()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }

    private class MapReduceIndexCorax : MapReduceIndexBase
    {
        public MapReduceIndexCorax()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class MapReduceIndexBase : AbstractIndexCreationTask<Dto, MapReduceIndexBase.Result>
    {
        public class Result
        {
            public string Id { get; set; }
            public object Vector { get; set; }
        }

        public MapReduceIndexBase()
        {
            Map = dtos => from doc in dtos
                select new Result() { Id = doc.Id, Vector = doc.Text };
            
            Reduce = results => from result in results
                group result by result.Id into g
                    select new Result()
                {
                    Id = g.Key, Vector = CreateVector(g.Select(x => (float[])x.Vector).ToArray()) 
                };
        }
    }
}
