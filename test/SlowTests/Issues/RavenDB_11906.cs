using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Highlighting;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_11906 : RavenTestBase
    {
        public RavenDB_11906(ITestOutputHelper output) : base(output)
        {
        }

        private class Index1 : AbstractIndexCreationTask<Item>
        {
            public Index1()
            {
                Map = items => from i in items
                               select new
                               {
                                   _ = CreateField(
                                       i.FieldName,
                                       i.FieldValue, new CreateFieldOptions
                                       {
                                           Indexing = FieldIndexing.Exact,
                                           Storage = i.Stored ? FieldStorage.Yes : FieldStorage.No,
                                           TermVector = null
                                       })
                               };
            }
        }

        private class Index1_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public Index1_JavaScript()
            {
                Maps = new HashSet<string>
                {
                    @"map('Items', function (i){
                                    return {
                                        _: { $value: i.FieldValue, $name: i.FieldName, $options: { indexing: 'Exact', storage: true, termVector: null }}
                                    };
                                })",
                };
            }
        }

        private class Index2 : AbstractIndexCreationTask<Item>
        {
            public Index2()
            {
                Map = items => from i in items
                               select new
                               {
                                   OtherField = i.OtherField,
                                   _ = CreateField(
                                       i.FieldName,
                                       i.FieldValue, new CreateFieldOptions
                                       {
                                           Indexing = FieldIndexing.Search,
                                           Storage = FieldStorage.Yes,
                                           TermVector = FieldTermVector.WithPositionsAndOffsets
                                       })
                               };

                // dynamic fields can inherit settings from default field options (__all_fields)
                Analyze(Constants.Documents.Indexing.Fields.AllFields, "StandardAnalyzer");
                Index(Constants.Documents.Indexing.Fields.AllFields, FieldIndexing.Search);

                // default field options can be overriden per field basis
                Index(x => x.OtherField, FieldIndexing.Default);
            }
        }

        private class Item
        {
            public string Id { get; set; }

            public string FieldName { get; set; }

            public string FieldValue { get; set; }

            public bool Stored { get; set; }

            public string OtherField { get; set; }
        }

        [Fact]
        public void SupportForCreateFieldWithOptions()
        {
            using (var store = GetDocumentStore())
            {
                new Index1().Execute(store);
                new Index1_JavaScript().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        FieldName = "F1",
                        FieldValue = "Value1",
                        Stored = true
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                var terms = store.Maintenance.Send(new GetTermsOperation(new Index1().IndexName, "F1", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("Value1", terms[0]);

                terms = store.Maintenance.Send(new GetTermsOperation(new Index1_JavaScript().IndexName, "F1", null));
                Assert.Equal(1, terms.Length);
                Assert.Equal("Value1", terms[0]);
            }
        }

        [Fact]
        public void CheckHighlighting()
        {
            using (var store = GetDocumentStore())
            {
                new Index2().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        FieldName = "Field",
                        FieldValue = "Itamar Syn-Hershko: Afraid of Map/Reduce? In this session, core RavenDB developer Itamar Syn-Hershko will walk through the RavenDB indexing process, grok it and much more."
                    });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var options = new HighlightingOptions
                    {
                        PreTags = new[] { "<span style='background: yellow'>" },
                        PostTags = new[] { "</span>" }
                    };

                    var results = session.Advanced
                        .DocumentQuery<Item, Index2>()
                        .WaitForNonStaleResults()
                        .Highlight("Field", 128, 2, options, out Highlightings fieldHighlighting)
                        .Search("Field", "session")
                        .ToArray();

                    Assert.Equal(1, results.Length);

                    var fragments = fieldHighlighting.GetFragments("items/1-A");

                    Assert.Equal(1, fragments.Length);
                    Assert.Contains("<span style='background: yellow'>session</span>", fragments[0]);
                }
            }
        }
    }
}
