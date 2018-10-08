using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11907 : RavenTestBase
    {
        [Fact]
        public void CanProjectFromCollectionNotInJson()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Document());
                    s.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Query<Document>()
                        .Select(x => new Result
                        {
                            HasTags = x.Tags.Any(),
                            OriginalData = x.Tags,
                            FilteredData = x.Tags.Where(t => t != null).ToArray(),

                            All = x.Tags.All(s => s != "a"),
                            Select = x.Tags.Select(t => t + ","),
                            Sum = x.Tags.Sum(t => t.Length),

                            Where = x.Tags.Where(t => t != null),
                            Contains = x.Tags.Contains("a"),
                            ToList = x.Tags.Select(t => t + ",").ToList(),

                            Concat = x.Tags.Concat(new[] { "a" }),
                            Avg = x.Tags.Average(s => s.Length),
                            Max = x.Tags.Select(s => s.Length).Max(),

                            Min = x.Tags.Select(s => s.Length).Min(),
                            StrMax = x.Tags.Max(),
                            Count = x.Tags.Length,

                            ToDictionary = x.Tags.ToDictionary(t => t.Length),
                            Reverse = x.Tags.Reverse(),
                            Distinct = x.Tags.Distinct()

                        })
                        .SingleOrDefault();

                    Assert.NotNull(doc);
                    Assert.False(doc.HasTags);
                    Assert.Null(doc.OriginalData);
                    Assert.Empty(doc.FilteredData);

                    Assert.True(doc.All);
                    Assert.Empty(doc.Select);
                    Assert.Equal(0, doc.Sum);
                    Assert.Empty(doc.Where);

                    Assert.False(doc.Contains);
                    Assert.Empty(doc.ToList);
                    Assert.Equal(new[]{"a"}, doc.Concat);
                    Assert.Equal(0, doc.Avg);

                    Assert.Equal(0, doc.Max);
                    Assert.Equal(0, doc.Min);
                    Assert.Null(doc.StrMax);

                    Assert.Equal(0, doc.Count);
                    Assert.Empty(doc.ToDictionary);
                    Assert.Empty(doc.Reverse);

                    Assert.Empty(doc.Distinct);
                }
            }
        }

        [Fact]
        public void ChainPropagationOnMissingCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Document());
                    s.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Query<Document>()
                        .Select(x => new Result
                        {
                            HasTags = x.Tags.Where(t => t != null).Any(t => t == "tag")
                        })
                        .SingleOrDefault();

                    Assert.False(doc?.HasTags);
                }
            }
        }

        private class Result
        {
            public Result()
            {
                OriginalData = Array.Empty<string>();
            }

            public bool HasTags { get; set; }
            public string[] OriginalData { get; set; }
            public string[] FilteredData { get; set; }

            public bool All { get; set; }
            public IEnumerable<string> Select { get; set; }
            public int Sum { get; set; }
            public IEnumerable<string> Where { get; set; }

            public bool Contains { get; set; }
            public List<string> ToList { get; set; }
            public IEnumerable<string> Concat { get; set; }

            public int Max { get; set; }
            public int Min { get; set; }
            public string StrMax { get; set; }

            public int Count { get; set; }
            public double Avg { get; set; }
            public Dictionary<int, string> ToDictionary { get; set; }

            public IEnumerable<string> Reverse { get; set; }
            public IEnumerable<string> Distinct { get; set; }

        }

        private class Document
        {
            public Document()
            {
                Tags = Array.Empty<string>();
            }

            public string Id { get; set; }
            public string[] Tags { get; set; }

            public bool ShouldSerializeTags()
            {
                return Tags != null && Tags.Length > 0;
            }
        }
    }
}
