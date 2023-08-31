using System.Collections.Generic;
using System.IO;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations.Backups;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19560 : RavenTestBase
{
    public RavenDB_19560(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public void BackwardCompatibilityForDictionaryProcessing()
    {
        var backupPath = NewDataPath(forceCreateDir: true);
        var file = Path.Combine(backupPath, "ravendb_19560.ravendb-snapshot");
        ExtractFile(file);
        using var store = GetDocumentStore();
        var db = GetDatabaseName();
        using var _ = Backup.RestoreDatabase(store, new RestoreBackupConfiguration {BackupLocation = backupPath, DatabaseName = db});
        var dict = new Dictionary<string, string>() {{"Name", "Maciej"}};

        {
            using var session = store.OpenSession(db);
            var legacyWay = session.Query<Example, LinqDictIndex>().Select(i => i.Dict).OfType<List<KeyValuePair<string, string>>>().Single().Single();
            Assert.Equal("Name", legacyWay.Key);
            Assert.Equal("MaciejTest", legacyWay.Value);
            
            var doc  = session.Query<Example, LinqDictIndex>().Single();
            doc.Dict["Name"] = "jan";
            doc.Name = "jan";
            session.Store(doc);
            session.SaveChanges();
        }
        
        Indexes.WaitForIndexing(store, db);

        {
            using var session = store.OpenSession(db);
            var legacyWay = session.Query<Example, LinqDictIndex>().Select(i => i.Dict).OfType<List<KeyValuePair<string, string>>>().Single().Single();
            Assert.Equal("Name", legacyWay.Key);
            Assert.Equal("janTest", legacyWay.Value);
        }
    }
    
    private static void ExtractFile(string path)
    {
        using (var file = File.Create(path))
        using (var stream = typeof(RavenDB_10404).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_19560.ravendb_19560.ravendb-snapshot"))
        {
            stream.CopyTo(file);
        }
    }
    [Fact]
    public void CanStoreAndGetDictionaryFromIndexViaLinq() => CanStoreAndGetDictionaryFromIndexViaBase<LinqDictIndex>();
    
    [Fact]
    public void CanStoreAndGetDictionaryFromIndexViaJavaScript() => CanStoreAndGetDictionaryFromIndexViaBase<JavaScriptDictIndex>();
    
    private void CanStoreAndGetDictionaryFromIndexViaBase<TIndex>() where TIndex : AbstractIndexCreationTask, new ()
    {
        var dict = new Dictionary<string, string>() {{"Name", "Maciej"}};
        using var store = GetDocumentStore();
        {
            using var session = store.OpenSession();
            session.Store(new Example(){Name = "Maciej", Dict = dict});
            session.SaveChanges();
        }
        
        new TIndex().Execute(store);
        Indexes.WaitForIndexing(store);
        {
            using var session = store.OpenSession();
            dict["Name"] += "Test";
            var item = session.Query<Example, TIndex>().Where(i => i.Dict == dict).Select(i => i.Dict).Single();
            Assert.True(item.ContainsKey("Name"));
            Assert.Equal("MaciejTest", item["Name"]);
        }
    }
    
    
    private class LinqDictIndex : AbstractIndexCreationTask<Example>
    {
        public LinqDictIndex()
        {
            Map = examples => from doc in examples
                select new {Dict = new Dictionary<string, string>() {{"Name", doc.Name + "Test"}}};
            Store(i => i.Dict, FieldStorage.Yes);
        }
    }
    
    private class JavaScriptDictIndex : AbstractJavaScriptIndexCreationTask
    {
        public JavaScriptDictIndex()
        {
            Maps = new HashSet<string>() {"map('Examples',function (p) { return { Dict: {'Name': p.Name+'Test'} };})",};
            Fields = new Dictionary<string, IndexFieldOptions>() {{"Dict", new IndexFieldOptions() {Storage = FieldStorage.Yes}}};
        }
    }

    private class Example
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public Dictionary<string, string> Dict { get; set; }
    }
}
