 using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22363 : RavenTestBase
{
    public RavenDB_22363(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Lucene | RavenTestCategory.JavaScript | RavenTestCategory.Indexes)]
    public void LuceneBackwardCompatibilityForJsDynamicFieldWithOption()
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var file = Path.Combine(backupPath, "RavenDB_22363.ravendb-snapshot");
        ExtractFile(file);
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Lucene));
        var db = GetDatabaseName();
        using var _ = Backup.RestoreDatabase(store, new RestoreBackupConfiguration {BackupLocation = backupPath, DatabaseName = db});
        WaitForUserToContinueTheTest(store, database:db);
        var index = new BackwardCompatibilityDynamicFieldJsSearch();
        var terms = store.Maintenance.ForDatabase(db).Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
        Assert.Equal(1, terms.Length);
        Assert.Equal("word1 word2 word3", terms[0]);

        using (var session = store.OpenSession(db))
        {
            session.Store(new Product(){Name = "Word4 Word5 Word6"});
            session.Advanced.WaitForIndexesAfterSaveChanges();
            session.SaveChanges();
        }
        
        terms = store.Maintenance.ForDatabase(db).Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
        Assert.Equal(2, terms.Length);
        terms.AsSpan().Sort();
        Assert.Equal("word1 word2 word3", terms[0]);
        Assert.Equal("word4 word5 word6", terms[1]);
        
        
        void ExtractFile(string path)
        {
            using (var fileStream = File.Create(path))
            using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_22363.RavenDB_22363.ravendb-snapshot"))
            {
                stream.CopyTo(fileStream);
            }
        }
    }

    [RavenFact(RavenTestCategory.Lucene | RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
    public void CanApplySearchAnalyzerOnLuceneDynamicFieldJavaScriptIndex()
    {
        using var store = GetDocumentStore(Options.ForSearchEngine(RavenSearchEngineMode.Lucene));
        var index = new BackwardCompatibilityDynamicFieldJsSearch();
        index.Execute(store);
        using var session = store.OpenSession();
        session.Advanced.WaitForIndexesAfterSaveChanges();
        session.Store(new Product(){Name = "Word1 Word2 Word3"});
        session.SaveChanges();
        
        var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
        terms.AsSpan().Sort();
        
        Assert.Equal("word1", terms[0]);
        Assert.Equal("word2", terms[1]);
        Assert.Equal("word3", terms[2]);
    }
    private class BackwardCompatibilityDynamicFieldJsSearch : AbstractJavaScriptIndexCreationTask
    {
        public BackwardCompatibilityDynamicFieldJsSearch()
        {
            Maps = new HashSet<string>()
            {
                @"map(""Products"", (product) => {
    return {
        _: createField('Name', product.Name, {
            indexing: 'Search',
            storage: false,
            termVector: null
        })
    };
})"
            };
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ProperlyAppliesDynamicFieldConfiguration_ExactIndexing(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Product() { Name = "Word1 Word2 Word3" });
            session.SaveChanges();
        }

        var index = new ProperlyApplyDynamicFieldConfiguration("Exact", false);
        index.Execute(store);

        Indexes.WaitForIndexing(store);

        var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
        Assert.Equal(1, terms.Length);
        Assert.Equal("Word1 Word2 Word3", terms[0]);
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ProperlyAppliesDynamicFieldConfiguration_SearchIndexing(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Product() { Name = "Word1 Word2 Word3" });
            session.SaveChanges();
        }

        var index = new ProperlyApplyDynamicFieldConfiguration("Search", false);
        index.Execute(store);

        Indexes.WaitForIndexing(store);

        var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
        Assert.Equal(3, terms.Length);
        terms.AsSpan().Sort();
        Assert.Equal("word1", terms[0]);
        Assert.Equal("word2", terms[1]);
        Assert.Equal("word3", terms[2]);
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ProperlyAppliesDynamicFieldConfiguration_DefaultIndexing(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Product() { Name = "Maciej" });
            session.SaveChanges();
        }

        var index = new ProperlyApplyDynamicFieldConfiguration("No", true, injectConst: "TEST");
        index.Execute(store);

        Indexes.WaitForIndexing(store);

        var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, "Name", fromValue: null));
        Assert.Equal(0, terms.Length);

        // Test projection:
        using (var session = store.OpenSession())
        {
            var projection = session.Advanced.DocumentQuery<string>(index.IndexName).SelectFields<string>("Name").First();

            Assert.Equal("TEST", projection);
        }
    }

    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void ProperlyAppliesDynamicFieldConfiguration_NoIndexingStorageYes(Options options)
    {
        using var store = GetDocumentStore(options);
        using var session = store.OpenSession();
        session.Store(new Dto("Test"));
        session.SaveChanges();
        var index = new CSharpIndex();
        index.Execute(store);
        Indexes.WaitForIndexing(store);
        var terms = store.Maintenance.Send(new GetTermsOperation(index.IndexName, nameof(Dto.Name), fromValue: null));
        Assert.Equal(0, terms.Length);

        var result = session.Advanced.DocumentQuery<Dto>(index.IndexName).SelectFields<string>(nameof(Dto.Name)).First();
        Assert.Equal("Test", result);
    }

    private record Dto(string Name, string Id = null);

    private class CSharpIndex : AbstractIndexCreationTask<Dto>
    {
        public CSharpIndex()
        {
            Map = dtos => from dto in dtos
                select new { Name = dto.Name };

            Store(x => x.Name, FieldStorage.Yes);
            Index(x => x.Name, FieldIndexing.No);
        }
    }
    
    [RavenTheory(RavenTestCategory.Indexes)]
    [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
    public void CanUseExplicitAndDynamicFieldsCreationInJavaScriptIndex(Options options)
    {
        using var store = GetDocumentStore(options);
        using (var session = store.OpenSession())
        {
            session.Store(new Product() { Name = "Maciej" });
            session.SaveChanges();
        }

        new IndexCanUseExplicitAndDynamicFieldsCreationInJavaScript().Execute(store);
        using (var session = store.OpenSession())
        {
            var projectionResult = session.Query<Product, IndexCanUseExplicitAndDynamicFieldsCreationInJavaScript>()
                .Customize(x => x.WaitForNonStaleResults())
                .Select(x => x.Name)
                .First();
            var terms = store.Maintenance.Send(new GetTermsOperation(new IndexCanUseExplicitAndDynamicFieldsCreationInJavaScript().IndexName, nameof(Dto.Name), fromValue: null));
            Assert.Equal(1, terms.Length);
            Assert.Equal("maciej", terms[0]);
            Assert.Equal("TEST", projectionResult);
        }
    }
    
    [RavenFact(RavenTestCategory.Lucene | RavenTestCategory.Indexes | RavenTestCategory.JavaScript)]
    public void NameCollisionsBetweenDynamicAndExplicitFieldsWontCauseIndexingHangOnIndexReplacement()
    {
        var newDataPath = NewDataPath();
        IOExtensions.DeleteDirectory(newDataPath);
        using var store = GetDocumentStore(new Options() { Path = newDataPath, RunInMemory = false, DeleteDatabaseOnDispose = true });
        using (var session = store.OpenSession())
        {
            session.Store(
                new Product()
                {
                    Name = "Unique T-Shirt",
                    Price = 20,
                    Attributes = new Dictionary<string, string>() { { "Color", "black" }, { "Author", "me" } },
                    FilterAttributes = new List<string>() { "Color" },
                    OrderedAt = DateTime.Parse("2023-03-01T12:00:00.0000000")
                }, "Products/");

            session.SaveChanges();
        }

        new Index().Execute(store);
        Indexes.WaitForIndexing(store);
        new IndexUpdate().Execute(store);
        Indexes.WaitForIndexing(store);
        var status = store.Maintenance.Send(new GetIndexingStatusOperation());
        Assert.Equal(IndexRunningStatus.Running, status.Indexes[0].Status);
    }

    private class Product
    {
        public string Name { get; set; }

        public long Price { get; set; }

        public Dictionary<string, string> Attributes { get; set; }

        public List<string> FilterAttributes { get; set; }

        public DateTime OrderedAt { get; set; }
    }

    private class Index : AbstractJavaScriptIndexCreationTask
    {
        public override string IndexName => "Index";

        public Index()
        {
            Maps = new HashSet<string>()
            {
                @"map(""Products"", (product) => { 
    return {  
        Name: product.CreationDate, 
        _: Object.entries(product.Attributes).map(([key, value]) => createField(key, value, { 
            indexing: 'Default', 
            storage: false, 
            termVector: null 
        })) 
    }; 
})"
            };
        }
    }

    private class IndexUpdate : AbstractJavaScriptIndexCreationTask
    {
        public override string IndexName => "Index";

        public IndexUpdate()
        {
            Maps = new HashSet<string>()
            {
                @"map(""Products"", (product) => {
    return {
        Name: product.Name,
        OrderedAt: product.OrderedAt,
        _: createField('OrderedAt', new Date(Date.parse(product.OrderedAt)).toLocaleString('de-DE', {
  day: 'numeric',
  month: 'long',
  year: 'numeric'
}), {indexing: 'No', storage: true, termVector: null })
    }
})"
            };
        }
    }
    
    private class IndexCanUseExplicitAndDynamicFieldsCreationInJavaScript : AbstractJavaScriptIndexCreationTask
    {
        public IndexCanUseExplicitAndDynamicFieldsCreationInJavaScript()
        {
            Maps = new HashSet<string>()
            {
                @"map(""Products"", (product) => {
    return {
        Name: product.Name,
        _: createField('Name', ""TEST"", {
            indexing: 'No',
            storage: true,
            termVector: null
        })
    };
})"
            };
        }
    }

    private class ProperlyApplyDynamicFieldConfiguration : AbstractJavaScriptIndexCreationTask
    {
        public ProperlyApplyDynamicFieldConfiguration(string indexing, bool storage, string injectConst = null)
        {
            var value = injectConst != null
                ? $"'{injectConst}'"
                : "product.Name";
            Maps = new HashSet<string>()
            {
                $$"""
                  map("Products", (product) => {
                      return {
                          _: createField('Name', {{value}}, {
                               indexing: '{{indexing}}',
                              storage: {{storage.ToString().ToLowerInvariant()}},
                              termVector: null
                          })
                      };
                  })
                  """
            };
        }
    }
}
