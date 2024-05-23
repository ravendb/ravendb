using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14797 : RavenTestBase
    {
        public RavenDB_14797(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Single)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.Sharded, Skip = "Output reduce to collection isn't supported")]
        public void ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new MapReduceWithOutputToCollection().Execute(store);
                new JavaIndex(options.SearchEngineMode is RavenSearchEngineMode.Corax).Execute(store);
                new JavaWithAdditionalSourcesIndex(options.SearchEngineMode is RavenSearchEngineMode.Corax).Execute(store);
                string entityId;
                using (var session = store.OpenSession())
                {
                    var entity = new Data() { Name = "1" };
                    session.Store(entity);
                    entityId = session.Advanced.GetDocumentId(entity);

                    session.Store(new Row
                    {
                        DataId = entityId,
                        LineNumber = 322
                    });

                    session.SaveChanges();
                }
                Indexes.WaitForIndexing(store);
                // we need to assert the indexes one by one, because each next index depends on previous 
                Assert.False(WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("MapReduceWithOutputToCollection")).IsStale, expectedVal: false, interval: 333), "1. MapReduceWithOutputToCollection");
                Assert.False(WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("JavaIndex")).IsStale, expectedVal: false, interval: 333), "1. JavaIndex");
                Assert.False(WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("JavaWithAdditionalSourcesIndex")).IsStale, expectedVal: false, interval: 333), "1. JavaWithAdditionalSourcesIndex");

                using (var session = store.OpenSession())
                {
                    var q1 = session.Query<ThirdOutput>(collectionName: "ThirdOutput");
                    var res = q1.FirstOrDefault();
                    WaitForUserToContinueTheTest(store);
                    Assert.NotNull(res.Communications.CommunicationType);
                    Assert.Equal("1", res.Communications.CommunicationType.Communication.Value);

                    var entity = session.Load<Data>(entityId);
                    entity.Name = "2";
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                Assert.False(WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("MapReduceWithOutputToCollection")).IsStale, expectedVal: false, interval: 333), "2. MapReduceWithOutputToCollection");
                Assert.False(WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("JavaIndex")).IsStale, expectedVal: false, interval: 333), "2. JavaIndex");
                Assert.False(WaitForValue(() => store.Maintenance.Send(new GetIndexStatisticsOperation("JavaWithAdditionalSourcesIndex")).IsStale, expectedVal: false, interval: 333), "2. JavaWithAdditionalSourcesIndex");

                using (var session = store.OpenSession())
                {
                    var q = session.Query<ThirdOutput>(collectionName: "ThirdOutput");
                    var res2 = q.ToList();
                    Assert.Equal("2", res2.First().Communications.CommunicationType.Communication.Value);
                }
            }
        }

        // RavenDB-14884
        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanCompileScriptWithSwitch(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Index_Rows().Execute(store);
                var id = "row/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Row
                    {
                        DataId = "Line/322",
                        LineNumber = 322
                    }, id);
                    session.SaveChanges();
                }
                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var entities = session.Query<Row, Index_Rows>().ProjectInto<Row>().ToList();
                    Assert.Equal(1, entities.Count);
                    Assert.Equal(1, entities.First().LineNumber);
                }
            }
        }

        private class Row
        {
            public string DataId { get; set; }
            public int LineNumber { get; set; }
        }

        private class Data
        {
            public string Name { get; set; }
        }

        private class FirstOutput
        {
            public string Name { get; set; }
            public string Email { get; set; }
            public int LineNumber { get; set; }
        }

        private class SecondOutput
        {
            public string TypeName { get; set; }
            public CommunicationValue Communication { get; set; }

            internal class CommunicationValue
            {
                public string Value { get; set; }
            }
        }

        private class ThirdOutput
        {
            public string Type { get; set; }
            public IncludedCommunications Communications { get; set; }

            internal class IncludedCommunications
            {
                public SecondOutput CommunicationType { get; set; }
            }
        }

        private class MapReduceWithOutputToCollection : AbstractIndexCreationTask<Row, FirstOutput>
        {
            public override string IndexName => "MapReduceWithOutputToCollection";

            public MapReduceWithOutputToCollection()
            {
                Map = rows => from row in rows
                              let doc = LoadDocument<Data>(row.DataId)
                              select new FirstOutput
                              {
                                  Name = doc.Name,
                                  Email = "Email",
                                  LineNumber = row.LineNumber
                              };

                Reduce = results => from result in results
                                    group result by new { result.LineNumber }
                    into g
                                    select new
                                    {
                                        Name = g.Select(x => x.Name).FirstOrDefault(),
                                        Email = g.Select(x => x.Email).FirstOrDefault(),
                                        LineNumber = g.Key.LineNumber
                                    };

                OutputReduceToCollection = @"FirstOutput";
            }
        }

        private class JavaIndex : AbstractJavaScriptIndexCreationTask
        {
            public override string IndexName => "JavaIndex";
            public JavaIndex(bool skipIndexingComplexFields)
            {
                Maps = new HashSet<string>
                {
                    @"map('FirstOutput', (row) => {
    return {
        Value: row.Name,
        TypeName: 'CommunicationType',
    };
})",
                };

                Reduce = @"groupBy(x => ({
    TypeName: x.TypeName
})).aggregate(g => {
    return {
        TypeName: g.key.TypeName,
        Communication: {
            Value: g.values.reduce((x, val) => val.Value, '')
        }
    };
})";

                OutputReduceToCollection = @"SecondOutput";
                PatternForOutputReduceToCollectionReferences = @"SecondOutput/References/{TypeName}";


                Fields = new Dictionary<string, IndexFieldOptions>
                {
                    {
                        Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions { Storage = FieldStorage.Yes }
                    },
                };
                
                if (skipIndexingComplexFields)
                    Fields.Add("Communication", new IndexFieldOptions(){Storage = FieldStorage.Yes, Indexing = FieldIndexing.No});
            }
        }

        private class JavaWithAdditionalSourcesIndex : AbstractJavaScriptIndexCreationTask
        {
            public override string IndexName => "JavaWithAdditionalSourcesIndex";
            public JavaWithAdditionalSourcesIndex(bool skipIndexingComplexFields)
            {
                Maps = new HashSet<string>
                {
                    @"map('FirstOutput', (row) => {
                    return {
                        Communications: includeCommunications(row, [""CommunicationType""]),
                        Type: 'Company',
                    };
                })"
                };

                Reduce = @"groupBy(x => ({
                    Type: x.Type
                    })).aggregate(g => {
                    return {
                        Type: g.key.Type,
                        Communications: g.values.reduce((x, val) => val.Communications, {})
                    };
                })";

                OutputReduceToCollection = @"ThirdOutput";

                if (skipIndexingComplexFields)
                {
                    Fields ??= new();
                    Fields.Add("Communications", new IndexFieldOptions(){Storage = FieldStorage.Yes, Indexing = FieldIndexing.No});
                }

                AdditionalSources = new Dictionary<string, string>
                {
                    ["The Script"] = @" function includeCommunications(doc, communicationNames) {
                                        var communications = {};
                                        for (var idx = 0; idx < communicationNames.length; idx++) {
                                            var related = 'SecondOutput/References/' + communicationNames[idx];
                                            var references = load(related, 'SecondOutput/References')['ReduceOutputs'];

                                            for (var i = 0; i < references.length; i++) {
                                                var match = load(references[i], 'SecondOutput');
                                                if (match != null) {
                                                    communications[match.TypeName] = match;
                                                }
                                            }
                                        }
                                        return communications;
                                    }"
                };
            }
        }

        private class Index_Rows : AbstractIndexCreationTask
        {
            public override string IndexName => "IndexRows";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"map(""Rows"", (row) => {
        return {
            DataId: row.DataId,
            LineNumber: getK(row)
        };
})"
                    },
                    AdditionalSources = new Dictionary<string, string>
                    {
                        ["The Script"] = @"function getK(doc) {
                                            var k;

                                            switch (typeof a) {
                                            default:
                                                k = 1
                                            }

                                            return k;
                                            }"
                    },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        {
                            Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions { Storage = FieldStorage.Yes }
                        }
                    }
            };
            }
        }
    }
}
