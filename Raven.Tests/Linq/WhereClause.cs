//-----------------------------------------------------------------------
// <copyright file="WhereClause.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Linq;
using Raven.Imports.Newtonsoft.Json;
using Xunit;

namespace Raven.Tests.Linq
{
	public class WhereClause : IDisposable
	{
		private readonly IDocumentStore documentStore;
		private readonly IDocumentSession documentSession;


		public WhereClause()
		{
			documentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize();
			documentSession = documentStore.OpenSession();
		}

		public class Renamed
		{
			[JsonProperty("Yellow")]
			public string Name { get; set; }
		} 

		[Fact]
		public void WillRespectRenames()
		{
			var q = documentSession.Query<Renamed>()
				.Where(x => x.Name == "red")
				.ToString();
			Assert.Equal("Yellow:red", q);
		}

		[Fact]
		public void HandlesNegative()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => !x.IsActive);
			Assert.Equal("IsActive:false", q.ToString());
		}

		[Fact]
		public void HandlesNegativeEquality()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => x.IsActive == false);
			Assert.Equal("IsActive:false", q.ToString());
		}

		[Fact]
		public void CanHandkeCasts()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => ((Dog)x.Animal).Color == "black");
			Assert.Equal("Animal.Color:black", q.ToString());
		}

		[Fact]
		public void StartsWith()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => user.Name.StartsWith("foo"));

			Assert.Equal("Name:foo*", q.ToString());
		}

		[Fact]
		public void StartsWithEqTrue()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => user.Name.StartsWith("foo") == true);

			Assert.Equal("Name:foo*", q.ToString());
		}

		[Fact]
		public void StartsWithEqFalse()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => user.Name.StartsWith("foo") == false);

			Assert.Equal("(*:* AND -Name:foo*)", q.ToString());
		}

		[Fact]
		public void StartsWithNegated()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => !user.Name.StartsWith("foo"));

			Assert.Equal("(*:* AND -Name:foo*)", q.ToString());
		}


		[Fact]
		public void IsNullOrEmpty()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => string.IsNullOrEmpty(user.Name));

			Assert.Equal("(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])", q.ToString());
		}

		[Fact]
		public void IsNullOrEmptyEqTrue()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => string.IsNullOrEmpty(user.Name) == true);

			Assert.Equal("(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])", q.ToString());
		}

		[Fact]
		public void IsNullOrEmptyEqFalse()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => string.IsNullOrEmpty(user.Name) == false);

			Assert.Equal("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.ToString());
		}

		[Fact]
		public void IsNullOrEmptyNegated()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => !string.IsNullOrEmpty(user.Name));

			Assert.Equal("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.ToString());
		}
		
		[Fact]
		public void IsNullOrEmpty_Any()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => user.Name.Any());

			Assert.Equal("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.ToString());
		}

		[Fact]
		public void IsNullOrEmpty_AnyEqTrue()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => user.Name.Any() == true);

			Assert.Equal("(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]]))", q.ToString());
		}

		[Fact]
		public void IsNullOrEmpty_AnyEqFalse()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => user.Name.Any() == false);

			Assert.Equal("(*:* AND -(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])))", q.ToString());
		}

		[Fact]
		public void IsNullOrEmpty_AnyNegated()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => user.Name.Any() == false);

			Assert.Equal("(*:* AND -(*:* AND -(Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])))", q.ToString());
			// Note: this can be generated also a smaller query: 
			// Assert.Equal("*:* AND (Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])", q.ToString());
		}

		[Fact]
		public void AnyWithPredicateShouldBeNotSupported()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => user.Name.Any(char.IsUpper));

			var exception = Assert.Throws<NotSupportedException>(() => q.ToString());
			Assert.Equal("Method not supported: Delegate.CreateDelegate. Expression: CreateDelegate(System.Func`2[System.Char,System.Boolean], null, Boolean IsUpper(Char)).", exception.Message);
		}

		[Fact]
		public void BracesOverrideOperatorPrecedence_second_method()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => user.Name == "ayende" && (user.Name == "rob" || user.Name == "dave"));

			Assert.Equal("Name:ayende AND (Name:rob OR Name:dave)", q.ToString());
			// Note: this can be generated also a smaller query: 
			// Assert.Equal("*:* AND (Name:[[NULL_VALUE]] OR Name:[[EMPTY_STRING]])", q.ToString());
		}

		[Fact]
		public void BracesOverrideOperatorPrecedence_third_method()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => user.Name == "ayende");
			q = q.Where(user => (user.Name == "rob" || user.Name == "dave"));

			Assert.Equal("(Name:ayende) AND (Name:rob OR Name:dave)", q.ToString());
		}

		[Fact]
		public void CanForceUsingIgnoreCase()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Name.Equals("ayende", StringComparison.InvariantCultureIgnoreCase)
					select user;
			Assert.Equal("Name:ayende", q.ToString());
		}

		[Fact]
		public void CanForceUsingCase()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Name.Equals("ayende", StringComparison.InvariantCulture)
					select user;
			Assert.Equal("Name:[[ayende]]", q.ToString());
		}

		[Fact]
		public void CanCompareValueThenPropertyGT()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where 15 > user.Age
					select user;
			Assert.Equal("Age_Range:{* TO Ix15}", q.ToString());
		}

		[Fact]
		public void CanCompareValueThenPropertyGE()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where 15 >= user.Age
					select user;
			Assert.Equal("Age_Range:[* TO Ix15]", q.ToString());
		}

		[Fact]
		public void CanCompareValueThenPropertyLT()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where 15 < user.Age
					select user;
			Assert.Equal("Age_Range:{Ix15 TO NULL}", q.ToString());
		}

		[Fact]
		public void CanCompareValueThenPropertyLE()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where 15 <= user.Age
					select user;
			Assert.Equal("Age_Range:[Ix15 TO NULL]", q.ToString());
		}

		[Fact]
		public void CanCompareValueThenPropertyEQ()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where 15 == user.Age
					select user;
			Assert.Equal("Age:15", q.ToString());
		}

		[Fact]
		public void CanCompareValueThenPropertyNE()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where 15 != user.Age
					select user;
			Assert.Equal("(-Age:15 AND Age:*)", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleEquality()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Name == "ayende"
					select user;
			Assert.Equal("Name:ayende", q.ToString());
		}

		private RavenQueryInspector<IndexedUser> GetRavenQueryInspector()
		{
			return (RavenQueryInspector<IndexedUser>)documentSession.Query<IndexedUser>();
		}

		private RavenQueryInspector<IndexedUser> GetRavenQueryInspectorStatic()
		{
			return (RavenQueryInspector<IndexedUser>)documentSession.Query<IndexedUser>("static");
		}

		[Fact]
		public void CanUnderstandSimpleEqualityWithVariable()
		{
			var indexedUsers = GetRavenQueryInspector();
			var ayende = "ayende" + 1;
			var q = from user in indexedUsers
					where user.Name == ayende
					select user;
			Assert.Equal("Name:ayende1", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleContains()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Name == ("ayende")
					select user;
			Assert.Equal("Name:ayende", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleContainsWithClauses()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from x in indexedUsers
					where x.Name == ("ayende")
					select x;


			Assert.NotNull(q);
			Assert.Equal("Name:ayende", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleContainsInExpresssion1()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from x in indexedUsers
					where x.Name == ("ayende")
					select x;

			Assert.NotNull(q);
			Assert.Equal("Name:ayende", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleContainsInExpresssion2()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from x in indexedUsers
					where x.Name == ("ayende")
					select x;

			Assert.NotNull(q);
			Assert.Equal("Name:ayende", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleStartsWithInExpresssion1()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from x in indexedUsers
					where x.Name.StartsWith("ayende")
					select x;

			Assert.NotNull(q);
			Assert.Equal("Name:ayende*", q.ToString());
		}


		[Fact]
		public void CanUnderstandSimpleStartsWithInExpresssion2()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from indexedUser in indexedUsers
					where indexedUser.Name.StartsWith("ayende")
					select indexedUser;

			Assert.NotNull(q);
			Assert.Equal("Name:ayende*", q.ToString());
		}


		[Fact]
		public void CanUnderstandSimpleContainsWithVariable()
		{
			var indexedUsers = GetRavenQueryInspector();
			var ayende = "ayende" + 1;
			var q = from user in indexedUsers
					where user.Name == (ayende)
					select user;
			Assert.Equal("Name:ayende1", q.ToString());
		}

		[Fact]
		public void NoOpShouldProduceEmptyString()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					select user;
			Assert.Equal("", q.ToString());
		}

		[Fact]
		public void CanUnderstandAnd()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Name == ("ayende") && user.Email == ("ayende@ayende.com")
					select user;
			Assert.Equal("Name:ayende AND Email:ayende@ayende.com", q.ToString());
		}

		[Fact]
		public void CanUnderstandOr()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Name == ("ayende") || user.Email == ("ayende@ayende.com")
					select user;
			Assert.Equal("Name:ayende OR Email:ayende@ayende.com", q.ToString());
		}

		[Fact]
		public void WithNoBracesOperatorPrecedenceIsHonoured()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Name == "ayende" && user.Name == "rob" || user.Name == "dave"
					select user;

			Assert.Equal("(Name:ayende AND Name:rob) OR Name:dave", q.ToString());
		}

		[Fact]
		public void BracesOverrideOperatorPrecedence()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Name == "ayende" && (user.Name == "rob" || user.Name == "dave")
					select user;

			Assert.Equal("Name:ayende AND (Name:rob OR Name:dave)", q.ToString());
		}

		[Fact]
		public void CanUnderstandLessThan()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday < new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:{* TO 2010-05-15T00:00:00.0000000}", q.ToString());
		}

		[Fact]
		public void NegatingSubClauses()
		{
			var query = ((IDocumentQuery<object>)new DocumentQuery<object>(null, null, null, null, null, null, null)).Not
				.OpenSubclause()
				.WhereEquals("IsPublished", true)
				.AndAlso()
				.WhereEquals("Tags.Length", 0)
				.CloseSubclause();
			Assert.Equal("-(IsPublished:true AND Tags.Length:0)", query.ToString());
		}

		[Fact]
		public void CanUnderstandEqualOnDate()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday == new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:2010-05-15T00:00:00.0000000", q.ToString());
		}

		[Fact]
		public void CanUnderstandLessThanOrEqualsTo()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday <= new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:[* TO 2010-05-15T00:00:00.0000000]", q.ToString());
		}

		[Fact]
		public void CanUnderstandGreaterThan()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday > new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:{2010-05-15T00:00:00.0000000 TO NULL}", q.ToString());
		}

		[Fact]
		public void CanUnderstandGreaterThanOrEqualsTo()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday >= new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
		}

		[Fact]
		public void CanUnderstandProjectionOfOneField()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday >= new DateTime(2010, 05, 15)
					select user.Name;
			Assert.Equal("<Name>: Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
		}

		[Fact]
		public void CanUnderstandProjectionOfMultipleFields()
		{
			var indexedUsers = GetRavenQueryInspector();
			var dateTime = new DateTime(2010, 05, 15);
			var q = from user in indexedUsers
					where user.Birthday >= dateTime
					select new { user.Name, user.Age };
			Assert.Equal("<Name, Age>: Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleEqualityOnInt()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Age == 3
					select user;
			Assert.Equal("Age:3", q.ToString());
		}


		[Fact]
		public void CanUnderstandGreaterThanOnInt()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Age > 3
					select user;
			Assert.Equal("Age_Range:{Ix3 TO NULL}", q.ToString());
		}

		[Fact]
		public void CanUnderstandMethodCalls()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday >= DateTime.Parse("2010-05-15")
					select new { user.Name, user.Age };
			Assert.Equal("<Name, Age>: Birthday:[2010-05-15T00:00:00.0000000 TO NULL]", q.ToString());
		}

		[Fact]
		public void CanUnderstandConvertExpressions()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Age == Convert.ToInt16("3")
					select user;
			Assert.Equal("Age:3", q.ToString());
		}


		[Fact]
		public void CanChainMultipleWhereClauses()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers
				.Where(x => x.Age == 3)
				.Where(x => x.Name == "ayende");
			Assert.Equal("(Age:3) AND (Name:ayende)", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleAny_Dynamic()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => x.Properties.Any(y => y.Key == "first"));
			Assert.Equal("Properties,Key:first", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleAny_Static()
		{
			var indexedUsers = GetRavenQueryInspectorStatic();
			var q = indexedUsers.Where(x => x.Properties.Any(y => y.Key == "first"));
			Assert.Equal("Properties_Key:first", q.ToString());
		}

		[Fact]
		public void AnyOnCollection()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => x.Properties.Any());
			Assert.Equal("Properties:*", q.ToString());
		}
		
		[Fact]
		public void AnyOnCollectionEqTrue()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => x.Properties.Any() == true);
			Assert.Equal("Properties:*", q.ToString());
		}

		[Fact]
		public void AnyOnCollectionEqFalse()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => x.Properties.Any() == false);
			Assert.Equal("(*:* AND -Properties:*)", q.ToString());
		}

		[Fact]
		public void AnyOnCollectionNegated()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => !x.Properties.Any());
			Assert.Equal("(*:* AND -Properties:*)", q.ToString());
		}

		[Fact]
		public void ComplexAnyWithPrecedingExpression()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(user => user.Name == null ||
									   !user.Properties.Any(property => property.Key == "Language" && property.Value != null));

			Assert.Equal(@"Name:[[NULL_VALUE]] OR (*:* AND -(Properties,Key:Language AND (-Properties,Value:[[NULL_VALUE]] AND Properties,Value:*)))", q.ToString());
		}

		[Fact]
		public void WillWrapLuceneSaveKeyword_NOT()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => x.Name == "NOT");
			Assert.Equal("Name:\"NOT\"", q.ToString());
		}

		[Fact]
		public void WillWrapLuceneSaveKeyword_OR()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => x.Name == "OR");
			Assert.Equal("Name:\"OR\"", q.ToString());
		}

		[Fact]
		public void WillWrapLuceneSaveKeyword_AND()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => x.Name == "AND");
			Assert.Equal("Name:\"AND\"", q.ToString());
		}

		[Fact]
		public void WillNotWrapCaseNotMatchedLuceneSaveKeyword_And()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers.Where(x => x.Name == "And");
			Assert.Equal("Name:And", q.ToString());
		}

		public class IndexedUser
		{
			public int Age { get; set; }
			public DateTime Birthday { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
			public UserProperty[] Properties { get; set; }
			public bool IsActive { get; set; }
			public IAnimal Animal { get; set; }
		}

		public interface IAnimal
		{
			
		}

		public class Dog : IAnimal
		{
			public string Color { get; set; }
		}

		public class UserProperty
		{
			public string Key { get; set; }
			public string Value { get; set; }
		}

		public void Dispose()
		{
			documentSession.Dispose();
			documentStore.Dispose();
		}
	}
}