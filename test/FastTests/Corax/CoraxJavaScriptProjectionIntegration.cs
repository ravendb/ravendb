using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Corax;

public class CoraxJavaScriptProjectionIntegration : RavenTestBase
{
    public CoraxJavaScriptProjectionIntegration(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.Querying)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.Corax)]
    public void CanPerformJsProjection(Options options)
    {
        using var store = GetDocumentStore(options);
        var dto = new Dto()
        {
            Long = 98L,
            Double = 98D,
            LongsListWithNulls = new long?[] {98L, null, 97L},
            DoublesListWithNulls = new double?[] {98D, null, 97D},
            StringListWithEmpty = new[] {"maciej", string.Empty, "kaszeba"},
            StringsListWithNulls = new[] {"maciej", null, "kaszeba"},
            RawObject = new() {Name = "maciej"}
        };
        
        
        using (var session = store.OpenSession())
        {
            session.Store(dto);
            
            session.SaveChanges();
        }

        var index = new TestIndex();
        index.Execute(store);
        WaitForUserToContinueTheTest(store);
        Indexes.WaitForIndexing(store);
        using (var session = store.OpenSession())
        {
            Assert.Equal(dto.Long + 100, GetResult<long>(session, nameof(dto.Long)));
            Assert.Equal(dto.Double + 100, GetResult<double>(session, nameof(dto.Double)));
            Assert.Equal(dto.LongsListWithNulls.Select(i => i == null ? i : i + 100).ToArray(), GetResult<long?[]>(session, nameof(dto.LongsListWithNulls)));
            Assert.Equal(dto.DoublesListWithNulls.Select(i => i == null ? i : i + 100).ToArray(), GetResult<double?[]>(session, nameof(dto.DoublesListWithNulls)));
            Assert.Equal(dto.StringListWithEmpty.Select(i => i == string.Empty ? i : i + "Stored").ToArray(), GetResult<string[]>(session, nameof(dto.StringListWithEmpty)));
            Assert.Equal(dto.StringsListWithNulls.Select(i => i == null ? i : i + "Stored").ToArray(), GetResult<string[]>(session, nameof(dto.StringsListWithNulls)));

            var innerObject = GetResult<Inner>(session, nameof(dto.RawObject));
            Assert.Equal(dto.RawObject.Name + "Stored", innerObject.Name);

        }

        T GetResult<T>(IDocumentSession session, string projectionPart)
        {
            return session.Advanced.RawQuery<Projection<T>>($"from index '{index.IndexName}' i select {{Test: i.{projectionPart}}}").Single().Test;
        }
        
    }
    
    // ReSharper disable once ClassNeverInstantiated.Local
    private record Projection<T>(T Test);
    
    private class TestIndex : AbstractIndexCreationTask<Dto>
    {
        public TestIndex()
        {
            Map = dtos => dtos.Select(i => new Dto
            {
                Id = i.Id,
                LongsListWithNulls = i.LongsListWithNulls.Select(i => i == null ? i : i + 100).ToArray(),
                DoublesListWithNulls = i.DoublesListWithNulls.Select(i => i == null ? i : i + 100).ToArray(),
                StringListWithEmpty = i.StringListWithEmpty.Select(i => i == string.Empty ? i : i + "Stored").ToArray(),
                StringsListWithNulls = i.StringsListWithNulls.Select(i => i == null ? i : i + "Stored").ToArray(),
                Long = i.Long + 100,
                Double = i.Double + 100,
                RawObject = new Inner() {
                    Name = i.RawObject.Name + "Stored"
                    }
            });
            
            Store(i => i.LongsListWithNulls, FieldStorage.Yes);
            Store(i => i.DoublesListWithNulls, FieldStorage.Yes);
            Store(i => i.StringListWithEmpty, FieldStorage.Yes);
            Store(i => i.StringsListWithNulls, FieldStorage.Yes);
            Store(i => i.Long, FieldStorage.Yes);
            Store(i => i.Double, FieldStorage.Yes);
            Store(i => i.RawObject, FieldStorage.Yes);
            Index(i => i.RawObject, FieldIndexing.No);
        }
    }
    
    private class Dto
    {
        public string Id { get; set; }
        public long?[] LongsListWithNulls { get; set; }
        public double?[] DoublesListWithNulls { get; set; }
        public string[] StringsListWithNulls { get; set; }
        public string[] StringListWithEmpty { get; set; }
        public long Long { get; set; }
        public double Double { get; set; }
        public Inner RawObject { get; set; }
    }
    
    private class Inner
    {
        public string Name { get; set; }
    }
}
