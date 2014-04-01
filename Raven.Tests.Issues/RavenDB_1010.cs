using System;
using System.Linq;

using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1010 : RavenTestBase
    {
        public class Foo
        {
            public TimeSpan Start { get; set; }
            public TimeSpan Until { get; set; }
            public int Age { get; set; }
        }

        [Fact]
        public void Ordering_By_TimeSpan_Property()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(11), Until = TimeSpan.FromHours(20) });
                    session.Store(new Foo { Start = TimeSpan.FromHours(15), Until = TimeSpan.FromHours(22) });
                    session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromHours(11) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .OrderBy(foo => foo.Start)
                                        .ToList();

                    Assert.NotNull(result);
                    Assert.Equal(3, result.Count);

                    Assert.Equal(TimeSpan.FromHours(10), result[0].Start);
                    Assert.Equal(TimeSpan.FromHours(11), result[1].Start);
                    Assert.Equal(TimeSpan.FromHours(15), result[2].Start);
                }
            }
        }

        [Fact]
        public void OrderingDescending_By_TimeSpan_Property()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(11), Until = TimeSpan.FromHours(20) });
                    session.Store(new Foo { Start = TimeSpan.FromHours(15), Until = TimeSpan.FromHours(22) });
                    session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromHours(11) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .OrderByDescending(foo => foo.Start)
                                        .ToList();

                    Assert.NotNull(result);
                    Assert.Equal(3, result.Count);

                    WaitForUserToContinueTheTest(documentStore);

                    Assert.Equal(TimeSpan.FromHours(15), result[0].Start);
                    Assert.Equal(TimeSpan.FromHours(11), result[1].Start);
                    Assert.Equal(TimeSpan.FromHours(10), result[2].Start);
                }
            }
        }

        [Fact]
        public void OrderingThenBy_By_TimeSpan_Property()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromHours(21) });
                    session.Store(new Foo { Start = TimeSpan.FromHours(11), Until = TimeSpan.FromHours(19) });
                    session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromHours(20) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .OrderBy(foo => foo.Start).ThenBy(foo => foo.Until)
                                        .ToList();

                    Assert.NotNull(result);
                    Assert.Equal(3, result.Count);

                    Assert.Equal(TimeSpan.FromHours(20), result[0].Until);
                    Assert.Equal(TimeSpan.FromHours(21), result[1].Until);
                    Assert.Equal(TimeSpan.FromHours(19), result[2].Until);
                }
            }
        }

        [Fact]
        public void OrderingThenByDescending_By_TimeSpan_Property()
        {
            using (var documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromHours(20) });
                    session.Store(new Foo { Start = TimeSpan.FromHours(11), Until = TimeSpan.FromHours(19) });
                    session.Store(new Foo { Start = TimeSpan.FromHours(10), Until = TimeSpan.FromHours(21) });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var result = session.Query<Foo>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .OrderBy(foo => foo.Start).ThenByDescending(foo => foo.Until)
                                        .ToList();

                    Assert.NotNull(result);
                    Assert.Equal(3, result.Count);

                    Assert.Equal(TimeSpan.FromHours(21), result[0].Until);
                    Assert.Equal(TimeSpan.FromHours(20), result[1].Until);
                    Assert.Equal(TimeSpan.FromHours(19), result[2].Until);
                }
            }
        }
    }
}