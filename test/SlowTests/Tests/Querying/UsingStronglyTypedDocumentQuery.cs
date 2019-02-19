using System;
using FastTests;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Tests.Querying
{
    public class UsingStronglyTypedDocumentQuery : RavenTestBase
    {
        private static IDocumentQuery<IndexedUser> CreateUserQuery()
        {
            return new DocumentQuery<IndexedUser>(null, "IndexName", null, false);
        }

        [Fact]
        public void WhereEqualsSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereEquals("Name", "ayende", exact: true).ToString(),
                CreateUserQuery().WhereEquals(x => x.Name, "ayende", exact: true).ToString());
            Assert.Equal(CreateUserQuery().WhereEquals("Name", "ayende").ToString(), CreateUserQuery()
                .WhereEquals(x => x.Name, "ayende").ToString());
        }

        [Fact]
        public void WhereInSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereIn("Name", new[] { "ayende", "tobias" }).ToString(),
                CreateUserQuery().WhereIn(x => x.Name, new[] { "ayende", "tobias" }).ToString());
        }

        [Fact]
        public void WhereStartsWithSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereStartsWith("Name", "ayende").ToString(),
                CreateUserQuery().WhereStartsWith(x => x.Name, "ayende").ToString());
        }

        [Fact]
        public void WhereEndsWithSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereEndsWith("Name", "ayende").ToString(),
                CreateUserQuery().WhereEndsWith(x => x.Name, "ayende").ToString());
        }

        [Fact]
        public void WhereBetweenSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereBetween("Name", "ayende", "zaphod").ToString(),
                CreateUserQuery().WhereBetween(x => x.Name, "ayende", "zaphod").ToString());
        }

        [Fact]
        public void WhereGreaterThanSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereGreaterThan("Birthday", new DateTime(1970, 01, 01)).ToString(),
                CreateUserQuery().WhereGreaterThan(x => x.Birthday, new DateTime(1970, 01, 01)).ToString());
        }

        [Fact]
        public void WhereGreaterThanOrEqualSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereGreaterThanOrEqual("Birthday", new DateTime(1970, 01, 01)).ToString(),
                CreateUserQuery().WhereGreaterThanOrEqual(x => x.Birthday, new DateTime(1970, 01, 01)).ToString());
        }

        [Fact]
        public void WhereLessThanSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereLessThan("Birthday", new DateTime(1970, 01, 01)).ToString(),
                CreateUserQuery().WhereLessThan(x => x.Birthday, new DateTime(1970, 01, 01)).ToString());
        }

        [Fact]
        public void WhereLessThanOrEqualSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().WhereLessThanOrEqual("Birthday", new DateTime(1970, 01, 01)).ToString(),
                CreateUserQuery().WhereLessThanOrEqual(x => x.Birthday, new DateTime(1970, 01, 01)).ToString());
        }

        [Fact]
        public void SearchSameAsUntypedCounterpart()
        {
            Assert.Equal(CreateUserQuery().Search("Name", "ayende").ToString(),
                CreateUserQuery().Search(x => x.Name, "ayende").ToString());
        }

        [Fact]
        public void CanUseStronglyTypedAddOrder()
        {
            CreateUserQuery().AddOrder(x => x.Birthday, false);
        }

        [Fact]
        public void CanUseStronglyTypedOrderBy()
        {
            CreateUserQuery().OrderBy(x => x.Birthday);
        }

        [Fact]
        public void CanUseStronglyTypedSearch()
        {
            CreateUserQuery().Search(x => x.Birthday, "1975");
        }

        [Fact]
        public void CanUseStronglyTypedGroupBy()
        {
        }

        private class IndexedUser
        {
            public int Age { get; set; }
            public DateTime Birthday { get; set; }
            public string Name { get; set; }
            public string Email { get; set; }
        }
    }
}
