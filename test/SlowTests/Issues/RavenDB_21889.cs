using System;
using System.IO;
using System.Linq;
using System.Net.Http;
using FastTests;
using FastTests.Corax;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Commands.Indexes;
using Sparrow;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21889 : RavenTestBase
{
    public RavenDB_21889(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Corax)]
    public void CoraxBackwardCompatibilityForSearchWithoutPhraseQuery()
    {
        const string fieldName = nameof(Item.FtsField) + global::Corax.Constants.PhraseQuerySuffixAsStr;
        var backupPath = NewDataPath(forceCreateDir: true);
        var fullBackupPath = Path.Combine(backupPath, "backup.ravendb-snapshot");

        using (var file = File.Create(fullBackupPath))
        using (var stream = typeof(RavenDB_19625).Assembly.GetManifestResourceStream("SlowTests.Data.RavenDB_21889.RavenDB_21889_BackwardCompatibility.ravendb-snapshot"))
        {
            stream!.CopyTo(file);
        }

        using var store = GetDocumentStore();
        var databaseName = GetDatabaseName();
        using var _ = Backup.RestoreDatabase(store, new RestoreBackupConfiguration {BackupLocation = backupPath, DatabaseName = databaseName});
        using var client = new HttpClient();
        string url = $"{store.Urls.First()}/databases/{databaseName}/indexes/debug?name={new Index().IndexName}&op=entries-fields&nodeTag=A";
        
        using (var session = store.OpenSession(databaseName))
        {
            session.Store(new Item(){FtsField = "word1 word2 word3 word4"});
            session.SaveChanges();
            Indexes.WaitForIndexing(store, databaseName);

            var query = session.Query<Item, Index>().Search(x => x.FtsField, "puppy word2").ToList();
            Assert.Equal(2, query.Count);

            query = session.Query<Item, Index>().Search(x => x.FtsField, "\"word3 word2\"").ToList();
            Assert.Equal(1, query.Count);
            
            Indexes.WaitForIndexing(store, databaseName);
            var fields = GetFields();
            
            Assert.DoesNotContain(fieldName, fields.Dynamic);
            Assert.DoesNotContain(fieldName, fields.Static);
            
            store.Maintenance.ForDatabase(databaseName).Send(new ResetIndexOperation(new Index().IndexName));
            Indexes.WaitForIndexing(store, databaseName: databaseName);

            fields = GetFields();
            Assert.Contains(fieldName, fields.Dynamic);
            
            query = session.Query<Item, Index>()
                .Customize(x => x.WaitForNonStaleResults())
                .Search(x => x.FtsField, "\"word3 word2\"")
                .ToList();

            Assert.Equal(0, query.Count);
        }

        Response GetFields()
        {
            var response = client.Send(new HttpRequestMessage(HttpMethod.Get, url)).Content;
            var data = response.ReadAsStream().ToByteArray();
            Assert.NotNull(data);
            return JsonConvert.DeserializeObject<Response>(Encodings.Utf8.GetString(data));
        }
    }

    private class Response
    {
        public string[] Static { get; set; }
        public string[] Dynamic { get; set; }
    }
    
    private class Item
    {
        public string FtsField { get; set; }
        public string Id { get; set; }
    }
    
    private class Index : AbstractIndexCreationTask<Item>
    {
        public Index()
        {
            Map = items => items.Select(x => new {FtsField = x.FtsField});

            Index(x => x.FtsField, FieldIndexing.Search);
        }
    }

}
