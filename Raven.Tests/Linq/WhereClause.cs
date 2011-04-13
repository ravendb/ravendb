//-----------------------------------------------------------------------
// <copyright file="WhereClause.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using System.Linq.Expressions;
using Raven.Client;
using Raven.Client.Client;
using Raven.Client.Document;
using Raven.Client.Embedded;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Tests.Linq
{
	public class WhereClause : IDisposable
	{
		private IDocumentStore documentStore;
		private IDocumentSession documentSession;


		public WhereClause()
		{
			documentStore = new EmbeddableDocumentStore
			{
				RunInMemory = true
			}.Initialize();
			documentSession = documentStore.OpenSession();
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
					where user.Name.Contains("ayende")
					select user;
			Assert.Equal("Name:ayende", q.ToString());
		}

        [Fact]
        public void CanUnderstandSimpleContainsWithClauses()
        {
            var indexedUsers = GetRavenQueryInspector();
        	var q = from x in indexedUsers
        	        where x.Name.Contains("ayende")
        	        select x;
                    

            Assert.NotNull(q);
            Assert.Equal("Name:ayende", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleContainsInExpresssion1()
        {
            var indexedUsers = GetRavenQueryInspector();
        	var q = from x in indexedUsers
        	        where x.Name.Contains("ayende")
        	        select x;

            Assert.NotNull(q);
            Assert.Equal("Name:ayende", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleContainsInExpresssion2()
        {
            var indexedUsers = GetRavenQueryInspector();
        	var q = from x in indexedUsers
        	        where x.Name.Contains("ayende")
        	        select x;

            Assert.NotNull(q);
            Assert.Equal("Name:ayende", q.ToString());
        }

        [Fact]
        public void CanUnderstandSimpleStartsWithInExpresssion1()
        {
            var indexedUsers = GetRavenQueryInspector();
        	var q = from x in indexedUsers
        	        where  x.Name.StartsWith("ayende")
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
					where user.Name.Contains(ayende)
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
					where user.Name.Contains("ayende") && user.Email.Contains("ayende@ayende.com")
					select user;
			Assert.Equal("Name:ayende AND Email:ayende@ayende.com", q.ToString());
		}

		[Fact]
		public void CanUnderstandOr()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Name.Contains("ayende") || user.Email.Contains("ayende@ayende.com")
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
					where user.Birthday < new DateTime(2010,05,15)
					select user;
			Assert.Equal("Birthday:{* TO 20100515000000000}", q.ToString());
		}

        [Fact]
        public void NegatingSubClauses()
        {
            var query = ((IDocumentQuery<object>)new DocumentQuery<object>(null, null, null, null, null, null)).Not
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
			Assert.Equal("Birthday:20100515000000000", q.ToString());
		}

		[Fact]
		public void CanUnderstandLessThanOrEqualsTo()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday <= new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:[* TO 20100515000000000]", q.ToString());
		}

		[Fact]
		public void CanUnderstandGreaterThan()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday > new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:{20100515000000000 TO NULL}", q.ToString());
		}

		[Fact]
		public void CanUnderstandGreaterThanOrEqualsTo()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday >= new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:[20100515000000000 TO NULL]", q.ToString());
		}

		[Fact]
		public void CanUnderstandProjectionOfOneField()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday >= new DateTime(2010, 05, 15)
					select user.Name;
			Assert.Equal("<Name>: Birthday:[20100515000000000 TO NULL]", q.ToString());
		}

		[Fact]
		public void CanUnderstandProjectionOfMultipleFields()
		{
			var indexedUsers = GetRavenQueryInspector();
			var dateTime = new DateTime(2010, 05, 15);
			var q = from user in indexedUsers
					where user.Birthday >= dateTime
					select new { user.Name, user.Age };
			Assert.Equal("<Name, Age>: Birthday:[20100515000000000 TO NULL]", q.ToString());
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
			Assert.Equal("Age_Range:{0x00000003 TO NULL}", q.ToString());
		}

		[Fact]
		public void CanUnderstandMethodCalls()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = from user in indexedUsers
					where user.Birthday >= DateTime.Parse("2010-05-15")
					select new { user.Name, user.Age };
			Assert.Equal("<Name, Age>: Birthday:[20100515000000000 TO NULL]", q.ToString());
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
			Assert.Equal("(Age:3) AND Name:ayende", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleAny()
		{
			var indexedUsers = GetRavenQueryInspector();
			var q = indexedUsers
				.Where(x => x.Properties.Any(y => y.Key == "first"));
			Assert.Equal("Key:first", q.ToString());
		}

		public class IndexedUser
		{
			public int Age { get; set; }
			public DateTime Birthday { get; set; }
			public string Name { get; set; }
			public string Email { get; set; }
			public UserProperty[] Properties { get; set; }
		}

		public class UserProperty
		{
			public string Key { get; set;}
		}

		public void Dispose()
		{
			documentSession.Dispose();
			documentStore.Dispose();
		}
	}
}
