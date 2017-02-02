//-----------------------------------------------------------------------
// <copyright file="WhereClause.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Client.Linq;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class WhereClause : RavenNewTestBase
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
                    var q = Queryable.Where(session.Query<Renamed>(), x => x.Name == "red")
                        .ToString();
                    Assert.Equal("Yellow:red", q);
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
                    Assert.Equal("IsActive:false", q.ToString());
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
                    Assert.Equal("IsActive:false", q.ToString());
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
                    Assert.Equal("Rate_Range:[Dx1246.43456538022 TO Dx1246.43456538023]", q.ToString());
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
                    Assert.Equal("Animal.Color:black", q.ToString());
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

                    Assert.Equal("Name:foo*", q.ToString());
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

                    Assert.Equal("Name:foo*", q.ToString());
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

                    Assert.Equal("(*:* AND -Name:foo*)", q.ToString());
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

                    Assert.Equal("(*:* AND -Name:foo*)", q.ToString());
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

                    Assert.Equal("(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])", q.ToString());
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

                    Assert.Equal("(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])", q.ToString());
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

                    Assert.Equal("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.ToString());
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

                    Assert.Equal("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.ToString());
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

                    Assert.Equal("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.ToString());
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

                    Assert.Equal("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.ToString());
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

                    Assert.Equal("(*:* AND -(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])))", q.ToString());
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

                    Assert.Equal("(*:* AND -(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])))", q.ToString());
                    // Note: this can be generated also a smaller query: 
                    // Assert.Equal("*:* AND (Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])", q.ToString());
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

                    Assert.Equal("Name:ayende AND (Name:rob OR Name:dave)", q.ToString());
                    // Note: this can be generated also a smaller query: 
                    // Assert.Equal("*:* AND (Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])", q.ToString());
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

                    Assert.Equal("(Name:ayende) AND (Name:rob OR Name:dave)", q.ToString());
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
                    Assert.Equal("Name:ayende", q.ToString());
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
                    Assert.Equal("Age_Range:{* TO Lx15}", q.ToString());
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
                    Assert.Equal("Age_Range:[* TO Lx15]", q.ToString());
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
                    Assert.Equal("Age_Range:{Lx15 TO NULL}", q.ToString());
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
                    Assert.Equal("Age_Range:[Lx15 TO NULL]", q.ToString());
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
                    Assert.Equal("Age:15", q.ToString());
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
                    Assert.Equal("(-Age:15 AND Age:*)", q.ToString());
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
                    Assert.Equal("Name:ayende", q.ToString());
                }
            }
        }

        private RavenQueryInspector<IndexedUser> GetRavenQueryInspector(IDocumentSession session)
        {
            return (RavenQueryInspector<IndexedUser>)session.Query<IndexedUser>();
        }

        private RavenQueryInspector<IndexedUser> GetRavenQueryInspectorStatic(IDocumentSession session)
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
                    Assert.Equal("Name:ayende1", q.ToString());
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
                    Assert.Equal("Name:ayende", q.ToString());
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
                    Assert.Equal("Name:ayende", q.ToString());
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
                    Assert.Equal("Name:ayende", q.ToString());
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
                    Assert.Equal("Name:ayende", q.ToString());
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
                    Assert.Equal("Name:ayende*", q.ToString());
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
                    Assert.Equal("Name:ayende*", q.ToString());
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
                    Assert.Equal("Name:ayende1", q.ToString());
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
                    Assert.Equal("", q.ToString());
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
                    Assert.Equal("Name:ayende AND Email:ayende@ayende.com", q.ToString());
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
                    Assert.Equal("Name:ayende OR Email:ayende@ayende.com", q.ToString());
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

                    Assert.Equal("(Name:ayende AND Name:rob) OR Name:dave", q.ToString());
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

                    Assert.Equal("Name:ayende AND (Name:rob OR Name:dave)", q.ToString());
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
                    Assert.Equal("Birthday:{* TO 2010-05-15T00:00:00.0000000}", q.ToString());
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
                    var query = ((IDocumentQuery<object>) new DocumentQuery<object>(null, null, null, null, false)).Not
                        .OpenSubclause()
                        .WhereEquals("IsPublished", true)
                        .AndAlso()
                        .WhereEquals("Tags.Length", 0)
                        .CloseSubclause();
                    Assert.Equal("-(IsPublished:true AND Tags.Length:0)", query.ToString());
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
                    Assert.Equal("Birthday:2010-05-15T00:00:00.0000000", q.ToString());
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
                    Assert.Equal("Birthday:[* TO 2010-05-15T00:00:00.0000000]", q.ToString());
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
                    Assert.Equal("Birthday:{2010-05-15T00:00:00.0000000 TO NULL}", q.ToString());
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
                    Assert.Equal("Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
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
                    Assert.Equal("<Name>: Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
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
                    Assert.Equal("<Name, Age>: Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
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
                    Assert.Equal("Age:3", q.ToString());
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
                    Assert.Equal("Age_Range:{Lx3 TO NULL}", q.ToString());
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
                            where user.Birthday >= DateTime.Parse("2010-05-15")
                            select new { user.Name, user.Age };
                    Assert.Equal("<Name, Age>: Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
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
                    Assert.Equal("Age:3", q.ToString());
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
                    Assert.Equal("(Age:3) AND (Name:ayende)", q.ToString());
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
                    Assert.Equal("Properties,Key:first", q.ToString());
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
                    Assert.Equal("Properties_Key:first", q.ToString());
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
                    Assert.Equal("Properties:*", q.ToString());
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
                    Assert.Equal("Properties:*", q.ToString());
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
                    Assert.Equal("(*:* AND -Properties:*)", q.ToString());
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
                    Assert.Equal("Name:\"NOT\"", q.ToString());
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
                    Assert.Equal("Name:\"OR\"", q.ToString());
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
                    Assert.Equal("Name:\"AND\"", q.ToString());
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
                    Assert.Equal("Name:And", q.ToString());
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
