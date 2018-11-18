using System;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12278 : RavenTestBase
    {
        [Fact]
        public void AggressivelyCachedSessionShouldGenerateProperCacheKeyForPostRequest()
        {
            using (var store = GetDocumentStore())
            {
                var documentCount = 50;

                var ids1 = Enumerable.Range(1, documentCount)
                    .Select(x => "my-quite-long-document-indentifier/" + x)
                    .ToList();

                var ids2 = Enumerable.Range(1, documentCount)
                    .Select(x => "my-quite-long-other-document-indentifier/" + x)
                    .ToList();

                using (var s = store.OpenSession())
                {
                    foreach (var id in ids1)
                    {
                        s.Store(new Document
                        {
                            Id = id
                        });
                    }

                    foreach (var id in ids2)
                    {
                        s.Store(new OtherDocument
                        {
                            Id = id
                        });
                    }

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    using (s.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(30000000)))
                    {
                        Assert.Equal(0, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                        var related = s.Load<Document>(ids1);
                        Assert.Equal(documentCount, related.Count(x => x.Value != null));
                        Assert.Equal(1, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                    }
                }
                using (var s = store.OpenSession())
                {
                    using (s.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(33000000)))
                    {
                        Assert.Equal(1, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                        var related = s.Load<OtherDocument>(ids2);
                        Assert.Equal(documentCount, related.Count(x => x.Value != null));
                        Assert.Equal(2, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                    }
                }

                using (var s = store.OpenSession())
                {
                    using (s.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(30000000)))
                    {
                        Assert.Equal(2, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                        var related = s.Load<Document>(ids1);
                        Assert.Equal(documentCount, related.Count(x => x.Value != null));
                        Assert.Equal(2, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                    }
                }

                using (var s = store.OpenSession())
                {
                    using (s.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(33000000)))
                    {
                        Assert.Equal(2, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                        var related = s.Load<OtherDocument>(ids2);
                        Assert.Equal(documentCount, related.Count(x => x.Value != null));
                        Assert.Equal(2, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                    }
                }
            }
        }

        [Fact]
        public void AggressivelyCachedSessionShouldGenerateProperCacheKeyForPostRequestUsingSameCollection()
        {
            using (var store = GetDocumentStore())
            {
                var documentFirstRange = 50;
                var documentSecondRange = 100;

                var ids1 = Enumerable.Range(1, documentFirstRange)
                    .Select(x => "my-quite-very-long-document-indentifier/" + x)
                    .ToList();

                var ids2 = Enumerable.Range(documentFirstRange + 1, documentSecondRange)
                    .Select(x => "my-quite-very-long-document-indentifier/" + x)
                    .ToList();

                using (var s = store.OpenSession())
                {
                    foreach (var id in ids1)
                    {
                        s.Store(new Document
                        {
                            Id = id
                        });
                    }

                    foreach (var id in ids2)
                    {
                        s.Store(new Document
                        {
                            Id = id
                        });
                    }
                    s.SaveChanges();
                }
                using (var s = store.OpenSession())
                {
                    using (s.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(30)))
                    {
                        Assert.Equal(0, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                        var related = s.Load<Document>(ids1);
                        Assert.Equal(documentFirstRange, related.Count(x => x.Value != null));
                        Assert.Equal(1, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                    }
                }
                using (var s = store.OpenSession())
                {
                    using (s.Advanced.DocumentStore.AggressivelyCacheFor(TimeSpan.FromSeconds(30)))
                    {
                        Assert.Equal(1, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                        var related = s.Load<Document>(ids2);
                        Assert.Equal(documentSecondRange, related.Count(x => x.Value != null));
                        Assert.Equal(2, s.Advanced.RequestExecutor.Cache.NumberOfItems);
                    }
                }
            }
        }

        private class Document
        {
            public string Id { get; set; }
        }

        private class OtherDocument
        {
            public string Id { get; set; }
        }
    }
}
