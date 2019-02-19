using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class WhereStringEquals : RavenTestBase
    {
        [Fact]
        public void QueryString_CaseSensitive_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    Assert.Throws<NotSupportedException>(() =>
                        session.Query<MyEntity>()
                            .Customize(customization => customization.WaitForNonStaleResults())
                            .Count(o => string.Equals(o.StringData, "Some data", StringComparison.Ordinal)));
                }
            }
        }

        [Fact]
        public void QueryString_IgnoreCase_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => string.Equals(o.StringData, "Some data", StringComparison.OrdinalIgnoreCase));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public void QueryString_WithoutSpecifyingTheComparisonType_ShouldJustWork()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => string.Equals(o.StringData, "Some data"));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public void QueryString_WithoutSpecifyingTheComparisonType_ShouldJustWork_InvertParametersOrder()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => string.Equals("Some data", o.StringData));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public void RegularStringEqual_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => "Some data" == o.StringData);

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public void ConstantStringEquals_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => "Some data".Equals(o.StringData));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public void StringEqualsConstant_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => o.StringData.Equals("Some data"));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public void StringEqualsConstant_IgnoreCase_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => "Some data".Equals(o.StringData, StringComparison.OrdinalIgnoreCase));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public void StringEqualsConstant_CaseSensitive_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    Assert.Throws<NotSupportedException>(() =>
                        session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => "Some data".Equals(o.StringData, StringComparison.Ordinal)));

                }
            }
        }

        [Fact]
        public void RegularStringEqual_CaseSensitive_ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    Assert.Throws<NotSupportedException>(() =>
                        session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResults())
                        .Count(o => o.StringData.Equals("Some data", StringComparison.Ordinal)));
                }
            }
        }

        private class MyEntity
        {
            public string StringData { get; set; }
        }

        private static void Fill(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new MyEntity { StringData = "Some data" });
                session.Store(new MyEntity { StringData = "Some DATA" });
                session.Store(new MyEntity { StringData = "Some other data" });
                session.SaveChanges();
            }
        }
    }
}
