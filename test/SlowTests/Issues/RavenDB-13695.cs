using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Issues
{
    public class RavenDB_13695 : RavenTestBase
    {
        public RavenDB_13695(ITestOutputHelper output) : base(output)
        {
        }

        private class NewsDocument
        {
            public List<string> AuthorNames { get; set; }
        }

        private class DocumentInfo
        {
        }

        private class NewsDocumentIndex : AbstractIndexCreationTask<NewsDocument, NewsDocumentIndex.Info>
        {
            public static string StaticIndexName = "ETIS/NewsDocumentIndex";

            public override string IndexName
            {
                get { return StaticIndexName; }
            }

            public class Info : DocumentInfo
            {
                public string AuthorNamesStr { get; set; }

            }

            public NewsDocumentIndex()
            {
                Map = docs => from d in docs
                              select new Info
                              {
                                  AuthorNamesStr = d.AuthorNames != null ? string.Join(", ", d.AuthorNames.OrderBy(x => x)) : string.Empty,
                              };


                AdditionalSources = new Dictionary<string, string>()
                {
                    ["DateTimeExtension4"] = @"

using System;
using System.Collections.Generic;
using System.Linq;

namespace ETIS
{
    public static class DateTimeExtension4
    {
        public static IEnumerable<int> GetYearsRepresented(List<DateTime?> startTimes, IEnumerable<DateTime?> endTimes)
        {
            if (startTimes == null || endTimes == null)
                return null;

            var years = new List<int>();
            var tuples = startTimes.Zip(endTimes, (x, y) => new Tuple<DateTime?, DateTime?>(x, y)).ToList();

            foreach (var pair in tuples)
            {
                if (!pair.Item1.HasValue)
                    return null;
                if (!pair.Item2.HasValue)
                    continue;
                var startYear = pair.Item1.Value.Year;
                var endYear = pair.Item2.Value.Year;
                if (endYear - startYear < 0)
                    continue;
                years.AddRange(Enumerable.Range(startYear, endYear - startYear + 1));
            }

            return years;
        }
    }
}

"
                };
            }
        }

        [Fact]
        public void CanCompileAdditionalSource()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new NewsDocument
                    {
                        AuthorNames = new List<string>
                        {
                            "garcia", "weir"
                        }
                    });

                    session.Store(new NewsDocument());

                    session.SaveChanges();

                }

                // should not throw
                new NewsDocumentIndex().Execute(store);

                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var q = session.Query<NewsDocumentIndex.Info, NewsDocumentIndex>()
                        .Where(doc => doc.AuthorNamesStr == "garcia, weir")
                        .ToList();

                    Assert.Single(q);
                }

            }
        }

        [Fact]
        public void CanCompileAdditionalSource_List()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new NewsDocument
                    {
                        AuthorNames = new List<string>
                        {
                            "garcia", "weir"
                        }
                    });

                    session.Store(new NewsDocument());

                    session.SaveChanges();

                }

                // should not throw
                var index = new NewsDocumentIndex
                {
                    AdditionalSources = new Dictionary<string, string>
                    {
                        ["DateTimeExtension"] = @"

using System;
using System.Collections.Generic;
using System.Linq;

namespace ETIS
{
    public static class DateTimeExtension
    {
        public static bool Foo(List<DateTime?> startTimes)
        {
            if (startTimes == null)
                return false;

            return startTimes.Count(x => x.Value.Year > 2000) < 10;
        }
    }
}

"
                    }
                };

                index.Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var q = session.Query<NewsDocumentIndex.Info, NewsDocumentIndex>()
                        .Where(doc => doc.AuthorNamesStr == "garcia, weir")
                        .ToList();

                    Assert.Single(q);
                }

            }
        }

        [Fact]
        public void CanCompileAdditionalSource_IList()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new NewsDocument
                    {
                        AuthorNames = new List<string>
                        {
                            "garcia", "weir"
                        }
                    });

                    session.Store(new NewsDocument());

                    session.SaveChanges();

                }

                // should not throw
                var index = new NewsDocumentIndex
                {
                    AdditionalSources = new Dictionary<string, string>
                    {
                        ["DateTimeExtension"] = @"

using System;
using System.Collections.Generic;
using System.Linq;

namespace ETIS
{
    public static class DateTimeExtension
    {
        public static bool Foo(IList<DateTime?> startTimes)
        {
            if (startTimes == null)
                return false;

            return startTimes.Count(x => x.Value.Year > 2000) < 10;
        }
    }
}

"
                    }
                };

                index.Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var q = session.Query<NewsDocumentIndex.Info, NewsDocumentIndex>()
                        .Where(doc => doc.AuthorNamesStr == "garcia, weir")
                        .ToList();

                    Assert.Single(q);
                }

            }
        }

        [Fact]
        public void CanCompileAdditionalSource_ICollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new NewsDocument
                    {
                        AuthorNames = new List<string>
                        {
                            "garcia", "weir"
                        }
                    });

                    session.Store(new NewsDocument());

                    session.SaveChanges();

                }

                // should not throw
                var index = new NewsDocumentIndex
                {
                    AdditionalSources = new Dictionary<string, string>
                    {
                        ["DateTimeExtension"] = @"

using System;
using System.Collections.Generic;
using System.Linq;

namespace ETIS
{
    public static class DateTimeExtension
    {
        public static bool Foo(ICollection<string> names)
        {
            if (names == null)
                return false;

            return names.Count(x => x != ""ayende"") < 10;
        }
    }
}

"
                    }
                };

                index.Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var q = session.Query<NewsDocumentIndex.Info, NewsDocumentIndex>()
                        .Where(doc => doc.AuthorNamesStr == "garcia, weir")
                        .ToList();

                    Assert.Single(q);
                }

            }
        }

        [Fact]
        public void CanCompileAdditionalSource_Hashset()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new NewsDocument
                    {
                        AuthorNames = new List<string>
                        {
                            "garcia", "weir"
                        }
                    });

                    session.Store(new NewsDocument());

                    session.SaveChanges();

                }

                // should not throw
                var index = new NewsDocumentIndex
                {
                    AdditionalSources = new Dictionary<string, string>
                    {
                        ["DateTimeExtension"] = @"

using System;
using System.Collections.Generic;
using System.Linq;

namespace ETIS
{
    public static class DateTimeExtension
    {
        public static bool Foo(HashSet<long> numbers)
        {
            if (numbers == null)
                return false;

            return numbers.Count(x => x > 100) < 10;
        }
    }
}

"
                    }
                };

                index.Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var q = session.Query<NewsDocumentIndex.Info, NewsDocumentIndex>()
                        .Where(doc => doc.AuthorNamesStr == "garcia, weir")
                        .ToList();

                    Assert.Single(q);
                }

            }
        }

        [Fact]
        public void CanCompileAdditionalSource_Dictionary()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new NewsDocument
                    {
                        AuthorNames = new List<string>
                        {
                            "garcia", "weir"
                        }
                    });

                    session.Store(new NewsDocument());

                    session.SaveChanges();

                }

                // should not throw
                var index = new NewsDocumentIndex
                {
                    AdditionalSources = new Dictionary<string, string>
                    {
                        ["DateTimeExtension"] = @"

using System;
using System.Collections.Generic;
using System.Linq;

namespace ETIS
{
    public static class DateTimeExtension
    {
        public static bool Foo(Dictionary<string, long> dictionary)
        {
            if (dictionary == null)
                return false;

            return dictionary.Count(x => x.Value > 100) < 10;
        }
    }
}

"
                    }
                };

                index.Execute(store);

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var q = session.Query<NewsDocumentIndex.Info, NewsDocumentIndex>()
                        .Where(doc => doc.AuthorNamesStr == "garcia, weir")
                        .ToList();

                    Assert.Single(q);
                }

            }
        }
    }
}
