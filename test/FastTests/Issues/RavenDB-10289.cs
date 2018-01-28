using System;
using System.Linq;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_10289 : RavenTestBase
    {
        private class TestView
        {
            public TestView[] Children { get; set; }
        }

        [Fact]
        public void CanProjectDefaultingToEmptyArray()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestView());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Query<TestView>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(x => new
                        {
                            Children = x.Children ?? new TestView[0]
                        })
                        .Single();

                    Assert.NotNull(doc.Children);
                    Assert.Equal(0, doc.Children.Length);
                }

                using (var session = store.OpenSession())
                { 
                    var doc = session.Query<TestView>()
                        .Select(x => new
                        {
                            Children = x.Children ?? Array.Empty<TestView>()
                        })
                        .Single();

                    Assert.NotNull(doc.Children);
                    Assert.Equal(0, doc.Children.Length);
                }
            }
        }

        [Fact]
        public void CanProjectDefaultingToNonEmptyArray()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestView());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Query<TestView>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(x => new
                        {
                            Children = x.Children ?? new TestView[3],
                            Nums = new int[4],
                            Bools = new bool[5]
                        })
                        .Single();

                    Assert.Equal(3, doc.Children.Length);
                    foreach (var testView in doc.Children)
                    {
                        Assert.Null(testView);
                    }

                    Assert.Equal(4, doc.Nums.Length);
                    foreach (var num in doc.Nums)
                    {
                        Assert.Equal(0, num);
                    }

                    Assert.Equal(5, doc.Bools.Length);
                    foreach (var b in doc.Bools)
                    {
                        Assert.False(b);
                    }

                }
            }
        }
    }
}
