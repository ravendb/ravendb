using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class WhereStringEquals : RavenTestBase
    {
        [Fact]
        public async Task QueryString_CaseSensitive_ShouldWork()
        {
            using (var store = await GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    Assert.Throws<NotSupportedException>(() =>
                        session.Query<MyEntity>()
                            .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                            .Count(o => string.Equals(o.StringData, "Some data", StringComparison.Ordinal)));
                }
            }
        }

        [Fact]
        public async Task QueryString_IgnoreCase_ShouldWork()
        {
            using (var store = await GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .Count(o => string.Equals(o.StringData, "Some data", StringComparison.OrdinalIgnoreCase));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public async Task QueryString_WithoutSpecifyingTheComparisonType_ShouldJustWork()
        {
            using (var store = await GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .Count(o => string.Equals(o.StringData, "Some data"));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public async Task QueryString_WithoutSpecifyingTheComparisonType_ShouldJustWork_InvertParametersOrder()
        {
            using (var store = await GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .Count(o => string.Equals("Some data", o.StringData));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public async Task RegularStringEqual_ShouldWork()
        {
            using (var store = await GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .Count(o => "Some data" == o.StringData);

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public async Task ConstantStringEquals_ShouldWork()
        {
            using (var store = await GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .Count(o => "Some data".Equals(o.StringData));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public async Task StringEqualsConstant_ShouldWork()
        {
            using (var store = await GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .Count(o => o.StringData.Equals("Some data"));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public async Task StringEqualsConstant_IgnoreCase_ShouldWork()
        {
            using (var store = await GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    var count = session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .Count(o => "Some data".Equals(o.StringData, StringComparison.OrdinalIgnoreCase));

                    Assert.Equal(2, count);
                }
            }
        }

        [Fact]
        public async Task StringEqualsConstant_CaseSensitive_ShouldWork()
        {
            using (var store = await GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    Assert.Throws<NotSupportedException>(() =>
                        session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .Count(o => "Some data".Equals(o.StringData, StringComparison.Ordinal)));

                }
            }
        }

        [Fact]
        public async Task RegularStringEqual_CaseSensitive_ShouldWork()
        {
            using (var store = await GetDocumentStore())
            {
                Fill(store);

                using (var session = store.OpenSession())
                {
                    Assert.Throws<NotSupportedException>(() =>
                        session.Query<MyEntity>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
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