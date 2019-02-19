using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class ReduceCanUseExtensionMethods : RavenTestBase
    {
        private class InputData
        {
            public string Tags;
        }

        private class Result
        {
#pragma warning disable 649
            public string[] Tags;
#pragma warning restore 649
        }

        [Fact]
        public void CanUseExtensionMethods()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinitionBuilder<InputData, Result>()
                {
                    Map = documents => from doc in documents
                                       let tags = ((string[])doc.Tags.Split(',', StringSplitOptions.None)).Select(s => s.Trim()).Where(s => !string.IsNullOrEmpty(s))
                                       select new Result()
                                       {
                                           Tags = tags.ToArray()
                                       }
                }.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "Hi";
                store.Maintenance.Send(new PutIndexesOperation(new[] { indexDefinition }));

                using (var session = store.OpenSession())
                {
                    session.Store(new InputData { Tags = "Little, orange, comment" });
                    session.Store(new InputData { Tags = "only-one" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Result>("Hi")
                        .Search(d => d.Tags, "only-one")
                        .As<InputData>()
                        .ToList();

                    Assert.Single(results);
                }
            }
        }

        [Fact]
        public void CorrectlyUseExtensionMethodsOnConvertedType()
        {
            var indexDefinition = new PainfulIndex { Conventions = new DocumentConventions() }.CreateIndexDefinition();
            var map = indexDefinition.Maps.First();
            Assert.Contains("((String[]) doc.Tags.Split(", map);
        }

        private class PainfulIndex : AbstractMultiMapIndexCreationTask<Result>
        {
            public PainfulIndex()
            {
                AddMap<InputData>(documents => from doc in documents
                                                   // Do not remove the redundant (string[]). 
                                                   // It's intentional here and intended to test the following parsing: ((string[])prop).Select(...)
                                               let tags = ((string[])doc.Tags.Split(',', StringSplitOptions.None)).Select(s => s.Trim()).Where(s => !string.IsNullOrEmpty(s))
                                               select new Result()
                                               {
                                                   Tags = tags.ToArray()
                                               });
            }
        }
    }
}
