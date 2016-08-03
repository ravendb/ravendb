//-----------------------------------------------------------------------
// <copyright file="WhereClause.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json;
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
        public async Task WillRespectRenames()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var q = session.Query<Renamed>()
                        .Where(x => x.Name == "red")
                        .ToString();
                    Assert.Equal("Yellow:red", q);
                }
            }
        }

        [Fact]
        public async Task HandlesNegative()
        {
            using (var store = await GetDocumentStore())
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
        public async Task HandlesNegativeEquality()
        {
            using (var store = await GetDocumentStore())
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
        public async Task HandleDoubleRangeSearch()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanHandleCasts()
        {
            using (var store = await GetDocumentStore())
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
        public async Task StartsWith()
        {
            using (var store = await GetDocumentStore())
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
        public async Task StartsWithEqTrue()
        {
            using (var store = await GetDocumentStore())
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
        public async Task StartsWithEqFalse()
        {
            using (var store = await GetDocumentStore())
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
        public async Task StartsWithNegated()
        {
            using (var store = await GetDocumentStore())
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
        public async Task IsNullOrEmpty()
        {
            using (var store = await GetDocumentStore())
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
        public async Task IsNullOrEmptyEqTrue()
        {
            using (var store = await GetDocumentStore())
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
        public async Task IsNullOrEmptyEqFalse()
        {
            using (var store = await GetDocumentStore())
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
        public async Task IsNullOrEmptyNegated()
        {
            using (var store = await GetDocumentStore())
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
        public async Task IsNullOrEmpty_Any()
        {
            using (var store = await GetDocumentStore())
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
        public async Task IsNullOrEmpty_Any_Negated_Not_Supported()
        {
            using (var store = await GetDocumentStore())
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
        public async Task IsNullOrEmpty_AnyEqTrue()
        {
            using (var store = await GetDocumentStore())
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
        public async Task IsNullOrEmpty_AnyEqFalse()
        {
            using (var store = await GetDocumentStore())
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
        public async Task IsNullOrEmpty_AnyNegated()
        {
            using (var store = await GetDocumentStore())
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
        public async Task AnyWithPredicateShouldBeNotSupported()
        {
            using (var store = await GetDocumentStore())
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
        public async Task BracesOverrideOperatorPrecedence_second_method()
        {
            using (var store = await GetDocumentStore())
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
        public async Task BracesOverrideOperatorPrecedence_third_method()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanForceUsingIgnoreCase()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanCompareValueThenPropertyGT()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanCompareValueThenPropertyGE()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanCompareValueThenPropertyLT()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanCompareValueThenPropertyLE()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanCompareValueThenPropertyEQ()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanCompareValueThenPropertyNE()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleEquality()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleEqualityWithVariable()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleContains()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleContainsWithClauses()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleContainsInExpression1()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleContainsInExpression2()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleStartsWithInExpression1()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleStartsWithInExpression2()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleContainsWithVariable()
        {
            using (var store = await GetDocumentStore())
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
        public async Task NoOpShouldProduceEmptyString()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandAnd()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandOr()
        {
            using (var store = await GetDocumentStore())
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
        public async Task WithNoBracesOperatorPrecedenceIsHonored()
        {
            using (var store = await GetDocumentStore())
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
        public async Task BracesOverrideOperatorPrecedence()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandLessThan()
        {
            using (var store = await GetDocumentStore())
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
        public async Task NegatingSubClauses()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var query = ((IDocumentQuery<object>)new DocumentQuery<object>(null, null, null, null, null, null, null, false)).Not
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
        public async Task CanUnderstandEqualOnDate()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandLessThanOrEqualsTo()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandGreaterThan()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandGreaterThanOrEqualsTo()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandProjectionOfOneField()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandProjectionOfMultipleFields()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleEqualityOnInt()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandGreaterThanOnInt()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandMethodCalls()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandConvertExpressions()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanChainMultipleWhereClauses()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleAny_Dynamic()
        {
            using (var store = await GetDocumentStore())
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
        public async Task CanUnderstandSimpleAny_Static()
        {
            using (var store = await GetDocumentStore())
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
        public async Task AnyOnCollection()
        {
            using (var store = await GetDocumentStore())
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
        public async Task AnyOnCollectionEqTrue()
        {
            using (var store = await GetDocumentStore())
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
        public async Task AnyOnCollectionEqFalse()
        {
            using (var store = await GetDocumentStore())
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
        public async Task WillWrapLuceneSaveKeyword_NOT()
        {
            using (var store = await GetDocumentStore())
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
        public async Task WillWrapLuceneSaveKeyword_OR()
        {
            using (var store = await GetDocumentStore())
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
        public async Task WillWrapLuceneSaveKeyword_AND()
        {
            using (var store = await GetDocumentStore())
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
        public async Task WillNotWrapCaseNotMatchedLuceneSaveKeyword_And()
        {
            using (var store = await GetDocumentStore())
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
