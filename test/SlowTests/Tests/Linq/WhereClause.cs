//-----------------------------------------------------------------------
// <copyright file="WhereClause.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Globalization;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class WhereClause : RavenTestBase
    {
        private class Renamed
        {
            [JsonProperty("Yellow")]
            public string Name { get; set; }
        }

        [Fact]
        public void WillRespectRenames()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var q = Queryable.Where(session.Query<Renamed>(), x => x.Name == "red");

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from Renameds where Yellow = $p0", iq.Query);
                    Assert.Equal("red", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void HandlesNegative()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(x => !x.IsActive);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where IsActive = $p0", iq.Query);
                    Assert.Equal(false, iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void HandlesNegativeEquality()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(x => x.IsActive == false);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where IsActive = $p0", iq.Query);
                    Assert.Equal(false, iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void HandleDoubleRangeSearch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    double min = 1246.434565380224, max = 1246.434565380226;
                    var q = indexedUsers.Where(x => x.Rate >= min && x.Rate <= max);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Rate between $p0 and $p1", q.ToString());
                    Assert.Equal(min, iq.QueryParameters["p0"]);
                    Assert.Equal(max, iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void CanHandleCasts()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(x => ((Dog)x.Animal).Color == "black");

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Animal.Color = $p0", q.ToString());
                    Assert.Equal("black", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void StartsWith()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => user.Name.StartsWith("foo"));

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where startsWith(Name, $p0)", iq.Query);
                    Assert.Equal("foo", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void StartsWithEqTrue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => user.Name.StartsWith("foo") == true);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where startsWith(Name, $p0)", iq.Query);
                    Assert.Equal("foo", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void StartsWithEqFalse()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => user.Name.StartsWith("foo") == false);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (true and not startsWith(Name, $p0))", iq.Query);
                    Assert.Equal("foo", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void StartsWithNegated()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => !user.Name.StartsWith("foo"));

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (true and not startsWith(Name, $p0))", iq.Query);
                    Assert.Equal("foo", iq.QueryParameters["p0"]);
                }
            }
        }


        [Fact]
        public void IsNullOrEmpty()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => string.IsNullOrEmpty(user.Name));

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (Name = $p0 or Name = $p1)", iq.Query);
                    Assert.Equal(null, iq.QueryParameters["p0"]);
                    Assert.Equal(string.Empty, iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void IsNullOrEmptyEqTrue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => string.IsNullOrEmpty(user.Name) == true);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (Name = $p0 or Name = $p1)", iq.Query);
                    Assert.Equal(null, iq.QueryParameters["p0"]);
                    Assert.Equal(string.Empty, iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void IsNullOrEmptyEqFalse()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => string.IsNullOrEmpty(user.Name) == false);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (true and not (Name = $p0 or Name = $p1))", iq.Query);
                    Assert.Equal(null, iq.QueryParameters["p0"]);
                    Assert.Equal(string.Empty, iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void IsNullOrEmptyNegated()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => !string.IsNullOrEmpty(user.Name));

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (true and not (Name = $p0 or Name = $p1))", iq.Query);
                    Assert.Equal(null, iq.QueryParameters["p0"]);
                    Assert.Equal(string.Empty, iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void IsNullOrEmpty_Any()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => user.Name.Any());

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (Name != $p0 and Name != $p1)", iq.Query);
                    Assert.Equal(null, iq.QueryParameters["p0"]);
                    Assert.Equal(string.Empty, iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void IsNullOrEmpty_Any_Negated_Not_Supported()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => !user.Name.Any());

                    var exception = Assert.Throws<InvalidOperationException>(() => q.ToString());
                    Assert.Equal("Cannot process negated Any(), see RavenDB-732 http://issues.hibernatingrhinos.com/issue/RavenDB-732", exception.Message);
                }
            }
        }

        [Fact]
        public void IsNullOrEmpty_AnyEqTrue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => user.Name.Any() == true);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (Name != $p0 and Name != $p1)", iq.Query);
                    Assert.Equal(null, iq.QueryParameters["p0"]);
                    Assert.Equal(string.Empty, iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void IsNullOrEmpty_AnyEqFalse()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => user.Name.Any() == false);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (true and not (Name != $p0 and Name != $p1))", iq.Query);
                    Assert.Equal(null, iq.QueryParameters["p0"]);
                    Assert.Equal(string.Empty, iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void IsNullOrEmpty_AnyNegated()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => user.Name.Any() == false);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (true and not (Name != $p0 and Name != $p1))", iq.Query);
                    Assert.Equal(null, iq.QueryParameters["p0"]);
                    Assert.Equal(string.Empty, iq.QueryParameters["p1"]);

                    // Note: this can be generated also a smaller query: 
                    // Assert.Equal("*:* and (Name:[[NULL_VALUE]] or Name:[[EMPTY_STRING]])", q.ToString());
                }
            }
        }

        [Fact]
        public void AnyWithPredicateShouldBeNotSupported()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => user.Name.Any(char.IsUpper));

                    var exception = Assert.Throws<NotSupportedException>(() => q.ToString());
                    Assert.Contains("Method not supported", exception.InnerException.Message);
                }
            }
        }

        [Fact]
        public void BracesOverrideOperatorPrecedence_second_method()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => user.Name == "ayende" && (user.Name == "rob" || user.Name == "dave"));

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0 and (Name = $p1 or Name = $p2)", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                    Assert.Equal("rob", iq.QueryParameters["p1"]);
                    Assert.Equal("dave", iq.QueryParameters["p2"]);

                    // Note: this can be generated also a smaller query: 
                    // Assert.Equal("*:* and (Name:[[NULL_VALUE]] or Name:[[EMPTY_STRING]])", q.ToString());
                }
            }
        }

        [Fact]
        public void BracesOverrideOperatorPrecedence_third_method()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(user => user.Name == "ayende");
                    q = q.Where(user => (user.Name == "rob" || user.Name == "dave"));

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (Name = $p0) and (Name = $p1 or Name = $p2)", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                    Assert.Equal("rob", iq.QueryParameters["p1"]);
                    Assert.Equal("dave", iq.QueryParameters["p2"]);
                }
            }
        }

        [Fact]
        public void CanForceUsingIgnoreCase()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Name.Equals("ayende", StringComparison.OrdinalIgnoreCase)
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanCompareValueThenPropertyGT()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where 15 > user.Age
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Age < $p0", iq.Query);
                    Assert.Equal(15, iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanCompareValueThenPropertyGE()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where 15 >= user.Age
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Age <= $p0", iq.Query);
                    Assert.Equal(15, iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanCompareValueThenPropertyLT()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where 15 < user.Age
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Age > $p0", iq.Query);
                    Assert.Equal(15, iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanCompareValueThenPropertyLE()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where 15 <= user.Age
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Age >= $p0", iq.Query);
                    Assert.Equal(15, iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanCompareValueThenPropertyEQ()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where 15 == user.Age
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Age = $p0", iq.Query);
                    Assert.Equal(15, iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanCompareValueThenPropertyNE()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where 15 != user.Age
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Age != $p0", iq.Query);
                    Assert.Equal(15, iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandSimpleEquality()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Name == "ayende"
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                }
            }
        }

        private static RavenQueryInspector<IndexedUser> GetRavenQueryInspector(IDocumentSession session)
        {
            return (RavenQueryInspector<IndexedUser>)session.Query<IndexedUser>();
        }

        private static RavenQueryInspector<IndexedUser> GetRavenQueryInspectorStatic(IDocumentSession session)
        {
            return (RavenQueryInspector<IndexedUser>)session.Query<IndexedUser>("static");
        }

        [Fact]
        public void CanUnderstandSimpleEqualityWithVariable()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var ayende = "ayende" + 1;
                    var q = from user in indexedUsers
                            where user.Name == ayende
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal(ayende, iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandSimpleContains()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Name == ("ayende")
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandSimpleContainsWithClauses()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from x in indexedUsers
                            where x.Name == ("ayende")
                            select x;

                    Assert.NotNull(q);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandSimpleContainsInExpression1()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from x in indexedUsers
                            where x.Name == ("ayende")
                            select x;

                    Assert.NotNull(q);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandSimpleContainsInExpression2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from x in indexedUsers
                            where x.Name == ("ayende")
                            select x;

                    Assert.NotNull(q);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandSimpleStartsWithInExpression1()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from x in indexedUsers
                            where x.Name.StartsWith("ayende")
                            select x;

                    Assert.NotNull(q);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where startsWith(Name, $p0)", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                }
            }
        }


        [Fact]
        public void CanUnderstandSimpleStartsWithInExpression2()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from indexedUser in indexedUsers
                            where indexedUser.Name.StartsWith("ayende")
                            select indexedUser;

                    Assert.NotNull(q);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where startsWith(Name, $p0)", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                }
            }
        }


        [Fact]
        public void CanUnderstandSimpleContainsWithVariable()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var ayende = "ayende" + 1;
                    var q = from user in indexedUsers
                            where user.Name == (ayende)
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal("ayende1", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void NoOpShouldProduceEmptyString()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            select user;
                    Assert.Equal("from IndexedUsers", q.ToString());
                }
            }
        }

        [Fact]
        public void CanUnderstandAnd()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Name == ("ayende") && user.Email == ("ayende@ayende.com")
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0 and Email = $p1", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                    Assert.Equal("ayende@ayende.com", iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandOr()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Name == ("ayende") || user.Email == ("ayende@ayende.com")
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0 or Email = $p1", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                    Assert.Equal("ayende@ayende.com", iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void WithNoBracesOperatorPrecedenceIsHonored()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Name == "ayende" && user.Name == "rob" || user.Name == "dave"
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (Name = $p0 and Name = $p1) or Name = $p2", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                    Assert.Equal("rob", iq.QueryParameters["p1"]);
                    Assert.Equal("dave", iq.QueryParameters["p2"]);
                }
            }
        }

        [Fact]
        public void BracesOverrideOperatorPrecedence()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Name == "ayende" && (user.Name == "rob" || user.Name == "dave")
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0 and (Name = $p1 or Name = $p2)", iq.Query);
                    Assert.Equal("ayende", iq.QueryParameters["p0"]);
                    Assert.Equal("rob", iq.QueryParameters["p1"]);
                    Assert.Equal("dave", iq.QueryParameters["p2"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandLessThan()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Birthday < new DateTime(2010, 05, 15)
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Birthday < $p0", iq.Query);
                    Assert.Equal(new DateTime(2010, 05, 15), iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void NegatingSubClauses()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var query = ((IDocumentQuery<object>)new DocumentQuery<object>(null, null, "IndexedUsers", false)).Not
                        .OpenSubclause()
                        .WhereEquals("IsPublished", true)
                        .AndAlso()
                        .WhereEquals("Tags.Length", 0)
                        .CloseSubclause();

                    var iq = query.GetIndexQuery();
                    Assert.Equal("from IndexedUsers where true and not (IsPublished = $p0 and Tags.Length = $p1)", iq.Query);
                    Assert.Equal(true, iq.QueryParameters["p0"]);
                    Assert.Equal(0, iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandEqualOnDate()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Birthday == new DateTime(2010, 05, 15)
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Birthday = $p0", q.ToString());
                    Assert.Equal(new DateTime(2010, 05, 15), iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandLessThanOrEqualsTo()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Birthday <= new DateTime(2010, 05, 15)
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Birthday <= $p0", q.ToString());
                    Assert.Equal(new DateTime(2010, 05, 15), iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandGreaterThan()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Birthday > new DateTime(2010, 05, 15)
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Birthday > $p0", q.ToString());
                    Assert.Equal(new DateTime(2010, 05, 15), iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandGreaterThanOrEqualsTo()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Birthday >= new DateTime(2010, 05, 15)
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Birthday >= $p0", q.ToString());
                    Assert.Equal(new DateTime(2010, 05, 15), iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandProjectionOfOneField()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Birthday >= new DateTime(2010, 05, 15)
                            select user.Name;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Birthday >= $p0 select Name", iq.Query);
                    Assert.Equal(new DateTime(2010, 05, 15), iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandProjectionOfMultipleFields()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var dateTime = new DateTime(2010, 05, 15);
                    var q = from user in indexedUsers
                            where user.Birthday >= dateTime
                            select new { user.Name, user.Age };

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Birthday >= $p0 select Name, Age", iq.Query);
                    Assert.Equal(new DateTime(2010, 05, 15), iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandSimpleEqualityOnInt()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Age == 3
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Age = $p0", iq.Query);
                    Assert.Equal(3, iq.QueryParameters["p0"]);
                }
            }
        }


        [Fact]
        public void CanUnderstandGreaterThanOnInt()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Age > 3
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Age > $p0", q.ToString());
                    Assert.Equal(3, iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandMethodCalls()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Birthday >= DateTime.Parse("2010-05-15", CultureInfo.InvariantCulture)
                            select new { user.Name, user.Age };

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Birthday >= $p0 select Name, Age", iq.Query);
                    Assert.Equal(new DateTime(2010, 05, 15), iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandConvertExpressions()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = from user in indexedUsers
                            where user.Age == Convert.ToInt16("3")
                            select user;

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Age = $p0", q.ToString());
                    Assert.Equal(3, iq.QueryParameters["p0"]);
                }
            }
        }


        [Fact]
        public void CanChainMultipleWhereClauses()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers
                        .Where(x => x.Age == 3)
                        .Where(x => x.Name == "ayende");

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (Age = $p0) and (Name = $p1)", iq.Query);
                    Assert.Equal(3, iq.QueryParameters["p0"]);
                    Assert.Equal("ayende", iq.QueryParameters["p1"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandSimpleAny_Dynamic()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(x => x.Properties.Any(y => y.Key == "first"));

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Properties[].Key = $p0", iq.Query);
                    Assert.Equal("first", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void CanUnderstandSimpleAny_Static()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspectorStatic(session);
                    var q = indexedUsers.Where(x => x.Properties.Any(y => y.Key == "first"));

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from index 'static' where Properties_Key = $p0", iq.Query);
                    Assert.Equal("first", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void AnyOnCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(x => x.Properties.Any());

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where exists(Properties)", iq.Query);
                }
            }
        }

        [Fact]
        public void AnyOnCollectionEqTrue()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(x => x.Properties.Any() == true);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where exists(Properties)", iq.Query);
                }
            }
        }

        [Fact]
        public void AnyOnCollectionEqFalse()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(x => x.Properties.Any() == false);

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where (true and not exists(Properties))", iq.Query);
                }
            }
        }

        [Fact]
        public void WillWrapLuceneSaveKeyword_NOT()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(x => x.Name == "NOT");

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal("NOT", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void WillWrapLuceneSaveKeyword_OR()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(x => x.Name == "OR");

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal("OR", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void WillWrapLuceneSaveKeyword_AND()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(x => x.Name == "AND");

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal("AND", iq.QueryParameters["p0"]);
                }
            }
        }

        [Fact]
        public void WillNotWrapCaseNotMatchedLuceneSaveKeyword_And()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var indexedUsers = GetRavenQueryInspector(session);
                    var q = indexedUsers.Where(x => x.Name == "And");

                    var iq = RavenTestHelper.GetIndexQuery(q);
                    Assert.Equal("from IndexedUsers where Name = $p0", iq.Query);
                    Assert.Equal("And", iq.QueryParameters["p0"]);
                }
            }
        }

        private class IndexedUser
        {
            public int Age { get; set; }
            public DateTime Birthday { get; set; }
            public string Name { get; set; }
            public string Email { get; set; }
            public UserProperty[] Properties { get; set; }
            public bool IsActive { get; set; }
            public IAnimal Animal { get; set; }
            public double Rate { get; set; }
        }

        private interface IAnimal
        {

        }

        private class Dog : IAnimal
        {
            public string Color { get; set; }
        }

        private class UserProperty
        {
            public string Key { get; set; }
            public string Value { get; set; }
        }
    }
}
