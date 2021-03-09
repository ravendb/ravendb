using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16334 : RavenTestBase
    {
        public RavenDB_16334(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [InlineData(true)]
        [InlineData(false)]
        public void CanWaitForIndexesWithLoadAfterSaveChanges(bool allIndexes)
        {
            using (var documentStore = GetDocumentStore())
            {
                new MyIndex().Execute(documentStore);
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new MainDocument() { Name = "A" });
                    session.Store(new RelatedDocument() { Name = "A", Value = 21.5m });
                    session.SaveChanges();
                }

                WaitForIndexing(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var result = session.Query<MyIndex.Result, MyIndex>().ProjectInto<MyIndex.Result>().Single();
                    Assert.Equal(21.5m, result.Value);
                }

                // Act
                using (var session = documentStore.OpenSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges(TimeSpan.FromSeconds(15), throwOnTimeout: true, indexes: allIndexes ? null : new[] { "MyIndex" });
                    var related = session.Load<RelatedDocument>("related/A");
                    related.Value = 42m;

                    session.SaveChanges();
                }

                // Assert
                using (var session = documentStore.OpenSession())
                {
                    var result = session.Query<MyIndex.Result, MyIndex>().ProjectInto<MyIndex.Result>().Single();
                    Assert.Equal(42m, result.Value);
                }
            }
        }

        public class MainDocument
        {
            public string Name { get; set; }
            public string Id => $"main/{Name}";
        }

        public class RelatedDocument
        {
            public string Name { get; set; }
            public decimal Value { get; set; }

            public string Id => $"related/{Name}";
        }

        public class MyIndex : AbstractIndexCreationTask<MainDocument>
        {
            public class Result
            {
                public string Name { get; set; }
                public decimal? Value { get; set; }
            }

            public MyIndex()
            {
                Map = mainDocuments => from mainDocument in mainDocuments
                                       let related = LoadDocument<RelatedDocument>($"related/{mainDocument.Name}")
                                       select new Result
                                       {
                                           Name = mainDocument.Name,
                                           Value = related != null ? related.Value : (decimal?)null
                                       };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
