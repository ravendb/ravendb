using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.NotificationCenter.Notifications;
using Sparrow.Json.Parsing;
using Sparrow.Server.Collections;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17097 : RavenTestBase
{
    public RavenDB_17097(ITestOutputHelper output) : base(output)
    {
    }

    private async Task NotificationTest<TIndex>(Options options, TIndex index)
        where TIndex : AbstractIndexCreationTask
    {
        using var store = GetDocumentStore(options);
        var db = await GetDatabase(store.Database);
        var notificationsQueue = new AsyncQueue<DynamicJsonValue>();
        using var _ = db.NotificationCenter.TrackActions(notificationsQueue, null);

        {
            using var s = store.OpenSession();
            s.Store(new CollectionDocument {Name = "Maciej", Inners = new[] {new InnerDoc {Name = "Doc1",}, new InnerDoc {Name = "Doc2"}}});
            s.Store(new CollectionDocument {Name = "Maciej1", Inners = new[] {new InnerDoc {Name = "Doc1",}, new InnerDoc {Name = "Doc2"}}});
            s.Store(new CollectionDocument {Name = "Maciej2", Inners = new[] {new InnerDoc {Name = "Doc1",}, new InnerDoc {Name = "Doc2"}}});

            s.SaveChanges();
        }

        await index.ExecuteAsync(store);
        await Indexes.WaitForIndexingAsync(store);

        Tuple<bool, DynamicJsonValue> performanceHint;

        do
        {
            performanceHint = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
        } while (performanceHint.Item2["Type"].ToString() != NotificationType.PerformanceHint.ToString());

        Assert.Equal($"Index '{index.IndexName}' is including the origin document in output.", performanceHint.Item2["Title"]);
        
        do
        {
            performanceHint = await notificationsQueue.TryDequeueAsync(TimeSpan.FromSeconds(5));
        } while (performanceHint.Item2 != null && performanceHint.Item2["Type"].ToString() != NotificationType.PerformanceHint.ToString());
        
        if (performanceHint.Item2 != null)
            Assert.NotEqual($"Index '{index.IndexName}' is including the origin document in output.", performanceHint.Item2["Title"]);

        WaitForUserToContinueTheTest(store);
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public Task FanOutIndexWithIncludedSourceDocumentWillSendNotification(Options options)
    {
        return NotificationTest<FanOutIndexWithSourceDocument>(options, new(options.SearchEngineMode is RavenSearchEngineMode.Corax));
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public Task IncludeDocumentDynamicField(Options options)
    {
        return NotificationTest<IncludeDocumentDynamicFieldIndex>(options, new(options.SearchEngineMode is RavenSearchEngineMode.Corax));
    }
    
    private class IncludeDocumentDynamicFieldIndex : AbstractIndexCreationTask<CollectionDocument>
    {
        public IncludeDocumentDynamicFieldIndex(bool skipIndexingComplexField)
        {
            Map = collectionDocuments =>
                from documentItself in collectionDocuments
                from inn in documentItself.Inners
                let x = 1
                let z = 2
                select new {Inner = inn, xx = x, zz = z, _ = CreateField("SourceDoc", documentItself, new() { Indexing = FieldIndexing.No, Storage = FieldStorage.Yes })};

            if (skipIndexingComplexField)
            {
                Index("Inner", FieldIndexing.No);
                Index("SourceDoc", FieldIndexing.No);
                Store("Inner", FieldStorage.Yes);
                Store("SourceDoc", FieldStorage.Yes);
            }
        }
    }
    
    private class FanOutIndexWithSourceDocument : AbstractIndexCreationTask<CollectionDocument>
    {
        public FanOutIndexWithSourceDocument(bool skipIndexingComplexField)
        {
            Map = collectionDocuments =>
                from documentItself in collectionDocuments
                from inn in documentItself.Inners
                let x = 1
                let z = 2
                select new {Field = documentItself, Inner = inn, xx = x, zz = z};
            
            if (skipIndexingComplexField)
            {
                Index("Inner", FieldIndexing.No);
                Index("Field", FieldIndexing.No);
                Store("Inner", FieldStorage.Yes);
                Store("Field", FieldStorage.Yes);
            }
        }
    }

    private class CollectionDocument
    {
        public string Name { get; set; }
        public InnerDoc[] Inners { get; set; }
    }

    private class InnerDoc
    {
        public string Name { get; set; }
    }
}
