using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2036 : RavenTestBase
    {
        public RavenDB_2036(ITestOutputHelper output) : base(output)
        {
        }

        public class Index__TestByName : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"from doc in docs select new { doc.Id, doc.Name, collection = doc[""@metadata""][""@collection""] };" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        {
                            "Name", new IndexFieldOptions { Indexing = FieldIndexing.Search }
                        }
                    }
                };
            }
        }

        public class DynamicByNameIndex : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"from doc in docs select new { doc.Id, doc.Name, collection = doc[""@metadata""][""@collection""] };" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        {
                            "Name", new IndexFieldOptions { Indexing = FieldIndexing.Search }
                        }
                    }
                };
            }
        }

        public class Dynamic : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"from doc in docs select new { doc.Id, doc.Name, collection = doc[""@metadata""][""@collection""] };" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        {
                            "Name", new IndexFieldOptions { Indexing = FieldIndexing.Search }
                        }
                    }
                };
            }
        }

        public class Dynamic_ : AbstractIndexCreationTask
        {
            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"from doc in docs select new { doc.Id, doc.Name, collection = doc[""@metadata""][""@collection""] };" },
                    Fields = new Dictionary<string, IndexFieldOptions>
                    {
                        {
                            "Name", new IndexFieldOptions { Indexing = FieldIndexing.Search }
                        }
                    }
                };
            }
        }


        [Fact]
        public void CheckDynamicName()
        {
            using (var store = GetDocumentStore())
            {
                new DynamicByNameIndex().Execute(store);
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {

                    var query = session
                        .Query<Tag, DynamicByNameIndex>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .OrderBy(n => n.Name)
                        .ToList();

                    Assert.Equal(query.Count, 0);
                }
            }
        }

        [Fact]
        public void DynamicUserDefinedIndexNameCreateWillNotFail()
        {
            using (var store = GetDocumentStore())
            {
                new Dynamic().Execute(store);

            }
        }

        [Fact]
        public void Dynamic_IndexNameCreateWillNotFail()
        {
            using (var store = GetDocumentStore())
            {
                new Dynamic_().Execute(store);
            }
        }

        [Fact]
        public void DynamicIndexOk()
        {
            using (var store = GetDocumentStore())
            {
                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session
                        .Query<Tag>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .OrderBy(n => n.Name)
                        .ToList();

                    Assert.Equal(query.Count, 0);
                }
            }
        }

        [Fact]
        public void DoubleUnderscoreIndexNameCreateFail()
        {
            using (var store = GetDocumentStore())
            {
                var ex = Assert.Throws<RavenException>(() => new Index__TestByName().Execute(store));
                Assert.True(ex.Message.Contains("Index name cannot contain // (double slashes)"));
            }
        }

        private class Tag
        {
            public string Name { get; set; }
        }
    }
}
