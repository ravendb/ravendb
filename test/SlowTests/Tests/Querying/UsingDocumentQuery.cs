//-----------------------------------------------------------------------
// <copyright file="UsingDocumentQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Document;
using Xunit;

namespace SlowTests.Tests.Querying
{
    public class UsingDocumentQuery : RavenNewTestBase
    {
        [Fact]
        public void CanUnderstandSimpleEquality()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereEquals("Name", "ayende", false);

            Assert.Equal("Name:[[ayende]]", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleEqualityWithVariable()
        {
            var ayende = "ayende" + 1;
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereEquals("Name", ayende, false);
            Assert.Equal("Name:[[ayende1]]", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleContains()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereIn("Name", new[] { "ayende" });
            Assert.Equal("@in<Name>:(ayende)", q.ToString());
        }

        [Fact]
        public void CanUnderstandParamArrayContains()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereIn("Name", new[] { "ryan", "heath" });
            Assert.Equal("@in<Name>:(ryan , heath)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContains()
        {
            var array = new[] { "ryan", "heath" };
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereIn("Name", array);
            Assert.Equal("@in<Name>:(ryan , heath)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContainsWithPhrase()
        {
            var array = new[] { "ryan", "heath here" };
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereIn("Name", array);
            Assert.Equal("@in<Name>:(ryan , \"heath here\")", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContainsWithOneElement()
        {
            var array = new[] { "ryan" };
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereIn("Name", array);
            Assert.Equal("@in<Name>:(ryan)", q.ToString());
        }

        [Fact]
        public void CanUnderstandArrayContainsWithZeroElements()
        {
            var array = new string[0];
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereIn("Name", array);
            Assert.Equal("@emptyIn<Name>:(no-results)", q.ToString());
        }

        [Fact]
        public void CanUnderstandEnumerableContains()
        {
            IEnumerable<string> list = new[] { "ryan", "heath" };
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereIn("Name", list);
            Assert.Equal("@in<Name>:(ryan , heath)", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleContainsWithVariable()
        {
            var ayende = "ayende" + 1;
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereIn("Name", new[] { ayende });
            Assert.Equal("@in<Name>:(ayende1)", q.ToString());
        }

        [Fact]
        public void NoOpShouldProduceEmptyString()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false));
            Assert.Equal("", q.ToString());
        }

        [Fact]
        public void CanUnderstandAnd()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereEquals("Name", "ayende")
                .AndAlso()
                .WhereEquals("Email", "ayende@ayende.com");
            Assert.Equal("Name:ayende AND Email:ayende@ayende.com", q.ToString());
        }

        [Fact]
        public void CanUnderstandOr()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereEquals("Name", "ayende")
                .OrElse()
                .WhereEquals("Email", "ayende@ayende.com");
            Assert.Equal("Name:ayende OR Email:ayende@ayende.com", q.ToString());
        }

        [Fact]
        public void CanUnderstandLessThan()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereLessThan("Birthday", new DateTime(2010, 05, 15));
            Assert.Equal("Birthday:{* TO 2010-05-15T00:00:00.0000000}", q.ToString());
        }

        [Fact]
        public void CanUnderstandEqualOnDate()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereEquals("Birthday", new DateTime(2010, 05, 15));
            Assert.Equal("Birthday:2010-05-15T00:00:00.0000000", q.ToString());
        }

        [Fact]
        public void CanUnderstandLessThanOrEqual()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereLessThanOrEqual("Birthday", new DateTime(2010, 05, 15));
            Assert.Equal("Birthday:[* TO 2010-05-15T00:00:00.0000000]", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThan()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereGreaterThan("Birthday", new DateTime(2010, 05, 15));
            Assert.Equal("Birthday:{2010-05-15T00:00:00.0000000 TO NULL}", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThanOrEqual()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereGreaterThanOrEqual("Birthday", new DateTime(2010, 05, 15));
            Assert.Equal("Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
        }

        [Fact]
        public void CanUnderstandProjectionOfSingleField()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereGreaterThanOrEqual("Birthday", new DateTime(2010, 05, 15))
                .SelectFields<IndexedUser>("Name") as DocumentQuery<IndexedUser>;
            string fields = q.GetProjectionFields().Any() ?
                "<" + String.Join(", ", q.GetProjectionFields().ToArray()) + ">: " : "";
            Assert.Equal("<Name>: Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", fields + q.ToString());
        }

        [Fact]
        public void CanUnderstandProjectionOfMultipleFields()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereGreaterThanOrEqual("Birthday", new DateTime(2010, 05, 15))
                .SelectFields<IndexedUser>("Name", "Age") as DocumentQuery<IndexedUser>;
            string fields = q.GetProjectionFields().Any() ?
                "<" + String.Join(", ", q.GetProjectionFields().ToArray()) + ">: " : "";
            Assert.Equal("<Name, Age>: Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", fields + q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleEqualityOnInt()
        {
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereEquals("Age", 3, false);
            Assert.Equal("Age:3", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThanOnInt()
        {
            // should DocumentQuery<T> understand how to generate range field names?
            var q = ((IDocumentQuery<IndexedUser>)new DocumentQuery<IndexedUser>(null, "IndexName", null, null, false))
                .WhereGreaterThan("Age_Range", 3);
            Assert.Equal("Age_Range:{Lx3 TO NULL}", q.ToString());
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
