using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents.Compilation;
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
        var exception = Assert.Throws<IndexCompilationException>(() => new MapReduceIndexCorax().Execute(store));
        Assert.Contains("The 'CreateVector' method is not supported in map-reduce indexes.", exception.Message);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDeployCoraxCsharpMapIndexWithVectorField(Options options)
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        new CsharpIndexBaseCorax().Execute(store);
        new LoadVectorCsharpIndexBaseCorax().Execute(store);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDeployCoraxJsMapIndexWithVectorField(Options options)
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        new JsIndexCorax().Execute(store);
        new LoadVectorJsIndexCorax().Execute(store);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDeployCoraxCsharpCounterIndexWithVectorField(Options options)
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        new CounterIndexCorax().Execute(store);
        new LoadVectorCounterIndexCorax().Execute(store);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanDeployCoraxCsharpTimeSeriesIndexWithVectorField(Options options)
    {
        options ??= Options.ForSearchEngine(RavenSearchEngineMode.Lucene);
        using var store = GetDocumentStore(options);
        new TimeSeriesIndexCorax().Execute(store);
        new LoadVectorTimeSeriesIndexCorax().Execute(store);
    }

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterIndexBase() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterTestBase<CounterIndexBase>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterIndexBaseLoadVector() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterTestBase<LoadVectorCounterIndexBase>();

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterIndexExplicitLucene(Options options) =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterTestBase<CounterIndexLucene>(options);

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterIndexExplicitLuceneLoadVector(Options options) =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpCounterTestBase<LoadVectorCounterIndexLucene>(options);

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesIndexBase() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesTestBase<TimeSeriesIndexBase>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesIndexBaseLoadVector() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesTestBase<LoadVectorTimeSeriesIndexBase>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesIndexExplicitLucene() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesTestBase<TimeSeriesIndexLucene>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesIndexExplicitLuceneLoadVector() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpTimeSeriesTestBase<LoadVectorTimeSeriesIndexLucene>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpMapIndexBase() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<CsharpIndexBase>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpMapIndexBaseLoadVector() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<LoadVectorCsharpIndexBase>();

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpMapIndexExplicitLucene(Options options) =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<CsharpIndexBaseLucene>(options);

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxCsharpMapIndexExplicitLuceneLoadVector(Options options) =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<LoadVectorCsharpIndexBaseLucene>(options);

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxJavaScriptBase() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<JsIndexBase>();

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxJavaScriptBaseLoadVector() =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<LoadVectorJsIndexBase>();

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxJavaScriptLucene(Options options) =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<JsIndexLucene>(options);

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxJavaScriptLuceneLoadVector(Options options) =>
        ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxBase<LoadVectorJsIndexLucene>(options);

    [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxMapReduceBase()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Lucene));
        var exception = Assert.Throws<IndexCompilationException>(() => new MapReduceIndexBase().Execute(store));
        Assert.Contains("The 'CreateVector' method is not supported in map-reduce indexes.", exception.Message);
    }

    [RavenTheory(RavenTestCategory.Vector | RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ThrowOnIndexCreationWithVectorFieldWhenSearchEngineIsNotCoraxMapReduceLucene(Options options)
    {
        using var store = GetDocumentStore(options);
        var exception = Assert.Throws<IndexCompilationException>(() => new MapReduceIndexLucene().Execute(store));
        Assert.Contains("The 'CreateVector' method is not supported in map-reduce indexes.", exception.Message);
    }

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
        Assert.Contains(
            "Vector fields are supported only by the Corax search engine. This deployment requested 'Lucene' search engine. Read more at https://ravendb.net/l/Y4B762/7.0",
            ravenException.InnerException.Message);
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
            Maps =
            [
                @$"map('Dtos', function (e) {{
    return {{ 
        Name: e.Name,
        Vector: createVector(e.Text)
    }};
}})"
            ];
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
                select new { HeartBeat = counter.Value, Name = counter.Name, User = counter.DocumentId, Vector = CreateVector(counter.Name) });
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
                    select new { HeartBeat = entry.Values[0], entry.Timestamp.Date, User = ts.DocumentId, Vector = CreateVector(ts.DocumentId) });
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
                group result by result.Id
                into g
                select new Result() { Id = g.Key, Vector = CreateVector(g.Select(x => (float[])x.Vector).ToArray()) };
        }
    }

    private class LoadVectorCsharpIndexBaseCorax : LoadVectorCsharpIndexBase
    {
        public LoadVectorCsharpIndexBaseCorax() : base()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class LoadVectorCsharpIndexBaseLucene : LoadVectorCsharpIndexBase
    {
        public LoadVectorCsharpIndexBaseLucene() : base()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }

    private class LoadVectorCsharpIndexBase : AbstractIndexCreationTask<Dto>
    {
        public LoadVectorCsharpIndexBase()
        {
            Map = dtos => dtos.Select(d => new { Vector = LoadVector("d.Text", "etlTaskName") });
        }
    }

    private class LoadVectorJsIndexCorax : LoadVectorJsIndexBase
    {
        public LoadVectorJsIndexCorax()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class LoadVectorJsIndexLucene : LoadVectorJsIndexBase
    {
        public LoadVectorJsIndexLucene()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }

    private class LoadVectorJsIndexBase : AbstractJavaScriptIndexCreationTask
    {
        public LoadVectorJsIndexBase()
        {
            Maps =
            [
                @$"map('Dtos', function (e) {{
    return {{ 
        Name: e.Name,
        Vector: loadVector('eText', 'etlId')
    }};
}})"
            ];
        }
    }

    private class LoadVectorCounterIndexCorax : LoadVectorCounterIndexBase
    {
        public LoadVectorCounterIndexCorax()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class LoadVectorCounterIndexLucene : LoadVectorCounterIndexBase
    {
        public LoadVectorCounterIndexLucene()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }

    private class LoadVectorCounterIndexBase : AbstractCountersIndexCreationTask<Company>
    {
        public LoadVectorCounterIndexBase()
        {
            AddMapForAll(counters => from counter in counters
                select new { HeartBeat = counter.Value, Name = counter.Name, User = counter.DocumentId, Vector = LoadVector("counter.Name", "etlTaskName") });
        }
    }

    private class LoadVectorTimeSeriesIndexCorax : LoadVectorTimeSeriesIndexBase
    {
        public LoadVectorTimeSeriesIndexCorax()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class LoadVectorTimeSeriesIndexLucene : LoadVectorTimeSeriesIndexBase
    {
        public LoadVectorTimeSeriesIndexLucene()
        {
            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Lucene;
        }
    }

    private class LoadVectorTimeSeriesIndexBase : AbstractTimeSeriesIndexCreationTask<Company>
    {
        public LoadVectorTimeSeriesIndexBase()
        {
            AddMap(
                "HeartRate",
                timeSeries => from ts in timeSeries
                    from entry in ts.Entries
                    select new { HeartBeat = entry.Values[0], entry.Timestamp.Date, User = ts.DocumentId, Vector = LoadVector("MyVec", "etlTaskName") });
        }
    }
}
