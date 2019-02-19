//-----------------------------------------------------------------------
// <copyright file="UsingDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Tests.Querying
{
    public class UsingDocumentQuery : RavenTestBase
    {
        [Fact]
        public void CanUnderstandSimpleEquality()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereEquals("Name", "ayende", exact: true);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where exact(Name = $p0)", q.ToString());
            Assert.Equal("ayende", query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandSimpleEqualityWithVariable()
        {
            var ayende = "ayende" + 1;
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereEquals("Name", ayende, exact: true);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where exact(Name = $p0)", q.ToString());
            Assert.Equal("ayende1", query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandSimpleContains()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereIn("Name", new[] { "ayende" });

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Name in ($p0)", q.ToString());
            Assert.Equal(new object[] { "ayende" }, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandParamArrayContains()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereIn("Name", new[] { "ryan", "heath" });

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Name in ($p0)", q.ToString());
            Assert.Equal(new object[] { "ryan", "heath" }, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandArrayContains()
        {
            var array = new[] { "ryan", "heath" };
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereIn("Name", array);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Name in ($p0)", q.ToString());
            Assert.Equal(new object[] { "ryan", "heath" }, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandArrayContainsWithPhrase()
        {
            var array = new[] { "ryan", "heath here" };
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereIn("Name", array);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Name in ($p0)", q.ToString());
            Assert.Equal(new object[] { "ryan", "heath here" }, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandArrayContainsWithOneElement()
        {
            var array = new[] { "ryan" };
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereIn("Name", array);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Name in ($p0)", q.ToString());
            Assert.Equal(new object [] {"ryan"}, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandArrayContainsWithZeroElements()
        {
            var array = new string[0];
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereIn("Name", array);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Name in ($p0)", q.ToString());
            Assert.Equal(new object[0], query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandEnumerableContains()
        {
            IEnumerable<string> list = new[] { "ryan", "heath" };
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereIn("Name", list);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Name in ($p0)", q.ToString());
            Assert.Equal(new object[] {"ryan", "heath"}, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandSimpleContainsWithVariable()
        {
            var ayende = "ayende" + 1;
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereIn("Name", new[] { ayende });

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Name in ($p0)", q.ToString());
            Assert.Equal(new object[] { "ayende1" }, query.QueryParameters["p0"]);
        }

        [Fact]
        public void NoOpShouldProduceEmptyString()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false));

            Assert.Equal("from index 'IndexName'", q.ToString());
        }

        [Fact]
        public void CanUnderstandAnd()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereEquals("Name", "ayende")
                .AndAlso()
                .WhereEquals("Email", "ayende@ayende.com");

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Name = $p0 and Email = $p1", q.ToString());
            Assert.Equal("ayende", query.QueryParameters["p0"]);
            Assert.Equal("ayende@ayende.com", query.QueryParameters["p1"]);
        }

        [Fact]
        public void CanUnderstandOr()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereEquals("Name", "ayende")
                .OrElse()
                .WhereEquals("Email", "ayende@ayende.com");

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Name = $p0 or Email = $p1", q.ToString());
            Assert.Equal("ayende", query.QueryParameters["p0"]);
            Assert.Equal("ayende@ayende.com", query.QueryParameters["p1"]);
        }

        [Fact]
        public void CanUnderstandLessThan()
        {
            var dateTime = new DateTime(2010, 05, 15);
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereLessThan("Birthday", dateTime);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Birthday < $p0", q.ToString());
            Assert.Equal(dateTime, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandEqualOnDate()
        {
            var dateTime = new DateTime(2010, 05, 15);
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereEquals("Birthday", dateTime);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Birthday = $p0", q.ToString());
            Assert.Equal(dateTime, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandLessThanOrEqual()
        {
            var dateTime = new DateTime(2010, 05, 15);
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereLessThanOrEqual("Birthday", dateTime);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Birthday <= $p0", q.ToString());
            Assert.Equal(dateTime, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandGreaterThan()
        {
            var dateTime = new DateTime(2010, 05, 15);
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereGreaterThan("Birthday", dateTime);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Birthday > $p0", q.ToString());
            Assert.Equal(dateTime, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandGreaterThanOrEqual()
        {
            var dateTime = new DateTime(2010, 05, 15);
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereGreaterThanOrEqual("Birthday", dateTime);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Birthday >= $p0", q.ToString());
            Assert.Equal(dateTime, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandProjectionOfSingleField()
        {
            var dateTime = new DateTime(2010, 05, 15);
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereGreaterThanOrEqual("Birthday", dateTime)
                .SelectFields<IndexedUser>("Name") as DocumentQuery<IndexedUser>;

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Birthday >= $p0 select Name", q.ToString());
            Assert.Equal(dateTime, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandProjectionOfMultipleFields()
        {
            var dateTime = new DateTime(2010, 05, 15);
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereGreaterThanOrEqual("Birthday", dateTime)
                .SelectFields<IndexedUser>("Name", "Age") as DocumentQuery<IndexedUser>;

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Birthday >= $p0 select Name, Age", q.ToString());
            Assert.Equal(dateTime, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandSimpleEqualityOnInt()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereEquals("Age", 3);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Age = $p0", q.ToString());
            Assert.Equal(3, query.QueryParameters["p0"]);
        }

        [Fact]
        public void CanUnderstandGreaterThanOnInt()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, false))
                .WhereGreaterThan("Age", 3);

            var query = q.GetIndexQuery();

            Assert.Equal("from index 'IndexName' where Age > $p0", q.ToString());
            Assert.Equal(3, query.QueryParameters["p0"]);
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
