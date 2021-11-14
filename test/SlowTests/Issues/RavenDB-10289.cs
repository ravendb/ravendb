using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10289 : RavenTestBase
    {
        public RavenDB_10289(ITestOutputHelper output) : base(output)
        {
        }

        private class TestView
        {
            public TestView[] Children { get; set; }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanProjectDefaultingToEmptyArray(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
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

        [Theory]
        [JavaScriptEngineClassData]
        public void CanProjectDefaultingToNonEmptyArray(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
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

        private class TestView2
        {
            public IEnumerable<TestView2> Children { get; set; }
            public IEnumerable<string> Names { get; set; }
            public Dictionary<string, string> Dictionary { get; set; }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanProjectDefaultingToEmptyList(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestView2());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Query<TestView2>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(x => new
                        {
                            emptyList = x.Children ?? new List<TestView2>()
                        })
                        .Single();

                    Assert.Equal(new List<TestView2>(), doc.emptyList);
                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanProjectToListWithParmeter(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestView2());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var list = new List<TestView2>
                    {
                        new TestView2()
                        {
                            Children = new List<TestView2>()
                        },
                        new TestView2()
                        {
                            Children = new List<TestView2>
                            {
                                new TestView2(), new TestView2()
                            }
                        }
                    };

                    var doc = session.Query<TestView2>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(x => new
                        {
                            collectionWithListParameter = x.Children ?? new Collection<TestView2>(list),
                            staticList = x.Children ?? list
                        })
                        .Single();


                    var collectionWithListParameter = doc.collectionWithListParameter.ToList();
                    Assert.Equal(2, collectionWithListParameter.Count);

                    Assert.Equal(0, collectionWithListParameter[0].Children.Count());
                    Assert.Null(collectionWithListParameter[0].Names);
                    Assert.Null(collectionWithListParameter[0].Dictionary);

                    Assert.Equal(2, collectionWithListParameter[1].Children.Count());
                    Assert.Null(collectionWithListParameter[1].Names);
                    Assert.Null(collectionWithListParameter[1].Dictionary);

                    var staticList = doc.staticList.ToList();
                    Assert.Equal(2, staticList.Count);

                    Assert.Equal(0, staticList[0].Children.Count());
                    Assert.Null(staticList[0].Names);
                    Assert.Null(staticList[0].Dictionary);

                    Assert.Equal(2, staticList[1].Children.Count());
                    Assert.Null(staticList[1].Names);
                    Assert.Null(staticList[1].Dictionary);

                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanProjectDefaultingToNewListWithInitializers(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestView2());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Query<TestView2>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(x => new
                        {
                            Children = x.Children ?? new List<TestView2>
                            {
                                new TestView2(),
                                new TestView2
                                {
                                    Children = new List<TestView2>()
                                    {
                                        new TestView2(), new TestView2()
                                    }
                                },
                                new TestView2
                                {
                                    Names = new[] {"john", "paul", "george", "ringo"}
                                }
                            }
                        })
                        .Single();


                    var children = doc.Children.ToList();
                    Assert.Equal(3, children.Count);

                    Assert.Null(children[0].Children);
                    Assert.Null(children[0].Names);
                    Assert.Null(children[0].Dictionary);

                    Assert.Equal(2, children[1].Children.Count());
                    Assert.Null(children[1].Dictionary);
                    Assert.Null(children[1].Names);

                    Assert.Null(children[2].Children);
                    Assert.Null(children[2].Dictionary);
                    Assert.Equal(4, children[2].Names.Count());

                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanProjectDefaultingToDictionary(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestView2());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Query<TestView2>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(x => new
                        {
                            Dictionary = x.Dictionary ?? new Dictionary<string, string>(),
                            DicWithInitializer = x.Dictionary ?? new Dictionary<string, string>()
                            {
                                {"a", "A" },
                                {"x", "X" }
                            }
                        })
                        .Single();

                    Assert.NotNull(doc.Dictionary);
                    Assert.Equal(0, doc.Dictionary.Count);

                    Assert.NotNull(doc.DicWithInitializer);
                    Assert.Equal(2, doc.DicWithInitializer.Count);

                    Assert.True(doc.DicWithInitializer.TryGetValue("a", out var val));
                    Assert.Equal("A", val);
                    Assert.True(doc.DicWithInitializer.TryGetValue("x", out val));
                    Assert.Equal("X", val);

                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanProjectDefaultingToHashSet(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestView2());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Query<TestView2>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(x => new
                        {
                            EmptyHashSet = x.Children ?? new HashSet<TestView2>(),
                            HashSetWithInitializers = x.Names ?? new HashSet<string>
                            {
                                "john", "paul", "george", "ringo"
                            }

                        })
                        .Single();

                    Assert.Equal(0, doc.EmptyHashSet.Count());

                    var hs = doc.HashSetWithInitializers.ToList();
                    Assert.Equal(4, hs.Count);

                    Assert.True(hs.Contains("john"));
                    Assert.True(hs.Contains("paul"));
                    Assert.True(hs.Contains("george"));
                    Assert.True(hs.Contains("ringo"));

                }
            }
        }
    }
}
