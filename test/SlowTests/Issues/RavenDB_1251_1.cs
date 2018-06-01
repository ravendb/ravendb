using System;
using System.Diagnostics;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_1251_1 : RavenTestBase
    {
        public class Foo
        {
            public TimeSpan Bar { get; set; }
        }

        [Fact]
        public void TimeSpan_Can_Sort_By_Range_Value()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo
                    {
                        Bar = TimeSpan.FromHours(-2)
                    });
                    session.Store(new Foo
                    {
                        Bar = TimeSpan.FromHours(-1)
                    });
                    session.Store(new Foo
                    {
                        Bar = TimeSpan.FromHours(0)
                    });
                    session.Store(new Foo
                    {
                        Bar = TimeSpan.FromHours(1)
                    });
                    session.Store(new Foo
                    {
                        Bar = TimeSpan.FromHours(2)
                    });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var q = session.Query<Foo>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .OrderByDescending(x => x.Bar);
                    Debug.WriteLine(q);
                    var result = q.ToList();

                    Assert.Equal(5, result.Count);
                    Assert.True(result[0].Bar > result[1].Bar);
                    Assert.True(result[1].Bar > result[2].Bar);
                    Assert.True(result[2].Bar > result[3].Bar);
                    Assert.True(result[3].Bar > result[4].Bar);
                }
            }
        }

        [Fact]
        public void TimeSpan_Can_Filter_By_Value()
        {
            using (var documentStore = GetDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new Foo
                    {
                        Bar = TimeSpan.FromHours(-2)
                    });
                    session.Store(new Foo
                    {
                        Bar = TimeSpan.FromHours(-1)
                    });
                    session.Store(new Foo
                    {
                        Bar = TimeSpan.FromHours(0)
                    });
                    session.Store(new Foo
                    {
                        Bar = TimeSpan.FromHours(1)
                    });
                    session.Store(new Foo
                    {
                        Bar = TimeSpan.FromHours(2)
                    });

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var t = TimeSpan.FromHours(-1.5);

                    var q = session.Query<Foo>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Bar > t)
                        .OrderByDescending(x => x.Bar);
                    Debug.WriteLine(q);
                    var result = q.ToList();

                    Assert.Equal(4, result.Count);
                    Assert.True(result[0].Bar > result[1].Bar);
                    Assert.True(result[1].Bar > result[2].Bar);
                    Assert.True(result[2].Bar > result[3].Bar);
                    Assert.Equal(TimeSpan.FromHours(-1), result[3].Bar);
                }
            }
        }
    }
}
