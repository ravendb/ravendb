using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Exceptions.Compilation;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2036 : RavenTestBase
    {
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
                            "Name", new IndexFieldOptions { Indexing = FieldIndexing.Analyzed }
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
                            "Name", new IndexFieldOptions { Indexing = FieldIndexing.Analyzed }
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
                            "Name", new IndexFieldOptions { Indexing = FieldIndexing.Analyzed }
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
                            "Name", new IndexFieldOptions { Indexing = FieldIndexing.Analyzed }
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
                WaitForIndexing(store);

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
        public void DynamicUserDefinedIndexNameCreateFail()
        {

            using (var store = GetDocumentStore())
            {
                var ex = Assert.Throws<RavenException>(() => new Dynamic().Execute(store));
                Assert.True(ex.Message.Contains("Index name dynamic is reserved!"));

            }
        }

        [Fact]
        public void Dynamic_IndexNameCreateFail()
        {
            using (var store = GetDocumentStore())
            {
                var ex = Assert.Throws<RavenException>(() => new Dynamic_().Execute(store));
                Assert.True(ex.Message.Contains("Index names starting with dynamic_ or dynamic/ are reserved!"));

            }
        }

        [Fact]
        public void DynamicIndexOk()
        {
            using (var store = GetDocumentStore())
            {
                WaitForIndexing(store);

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
