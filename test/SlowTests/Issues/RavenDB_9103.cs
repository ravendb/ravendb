using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9103 : RavenTestBase
    {
        public RavenDB_9103(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void ProjectingFromIndexFieldWithGroupPropertyShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Model_Info();
                store.ExecuteIndex(index);
                using (var session = store.OpenSession())
                {
                    session.Store(new Model
                    {
                        Id = Guid.NewGuid().ToString(),
                        Name = nameof(Model.Name),
                        Group = nameof(Model.Group),
                    });
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);
                    var infos = session.Query<ModelInfo, Model_Info>()
                        .ProjectInto<ModelInfo>()
                        .ToList();
                }
            }
        }
        
        public class Model
        {
            public string Id { get; set; }

            public string Name { get; set; }
            public string Group { get; set; }
        }

        public class ModelInfo
        {
            public string Name { get; set; }
            public string Group { get; set; }
        }

        public class Model_Info : AbstractIndexCreationTask<Model>
        {
            public Model_Info()
            {
                Map = list => list
                    .Select(x => new
                    {
                        x.Name,
                        x.Group
                    });

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
