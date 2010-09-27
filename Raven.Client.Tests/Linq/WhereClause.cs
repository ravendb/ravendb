using System;
using System.Linq;
using Raven.Client.Linq;
using Xunit;

namespace Raven.Client.Tests.Linq
{
    public class WhereClause
    {
        [Fact]
        public void CanUnderstandSimpleEquality()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
            var q = from user in indexedUsers
                    where user.Name == "ayende"
                    select user;
            Assert.Equal("Name:[[ayende]]", q.ToString());
        }

		[Fact]
		public void CanUnderstandSimpleEqualityWithVariable()
		{
			var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
			var ayende = "ayende" + 1;
			var q = from user in indexedUsers
					where user.Name == ayende
					select user;
			Assert.Equal("Name:[[ayende1]]", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleContains()
		{
			var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
			var q = from user in indexedUsers
					where user.Name.Contains("ayende")
					select user;
			Assert.Equal("Name:ayende", q.ToString());
		}

		[Fact]
		public void CanUnderstandSimpleContainsWithVariable()
		{
			var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
			var ayende = "ayende" + 1;
			var q = from user in indexedUsers
					where user.Name.Contains(ayende)
					select user;
			Assert.Equal("Name:ayende1", q.ToString());
		}

		[Fact]
		public void NoOpShouldProduceEmptyString()
		{
			var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
			var q = from user in indexedUsers
					select user;
			Assert.Equal("", q.ToString());
		}

        [Fact]
        public void CanUnderstandAnd()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
                    where user.Name.Contains("ayende") && user.Email.Contains("ayende@ayende.com")
                    select user;
            Assert.Equal("Name:ayende AND Email:ayende@ayende.com", q.ToString());
        }

        [Fact]
        public void CanUnderstandOr()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
                    where user.Name.Contains("ayende") || user.Email.Contains("ayende@ayende.com")
                    select user;
            Assert.Equal("Name:ayende OR Email:ayende@ayende.com", q.ToString());
        }

        [Fact]
        public void CanUnderstandLessThan()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
					where user.Birthday < new DateTime(2010,05,15)
                    select user;
			Assert.Equal("Birthday:{* TO 20100515000000000}", q.ToString());
        }

		[Fact]
		public void CanUnderstandEqualOnDate()
		{
			var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
			var q = from user in indexedUsers
					where user.Birthday == new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:20100515000000000", q.ToString());
		}

        [Fact]
        public void CanUnderstandLessThanOrEqualsTo()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
					where user.Birthday <= new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:[* TO 20100515000000000]", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThan()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
					where user.Birthday > new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:{20100515000000000 TO NULL}", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThanOrEqualsTo()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
					where user.Birthday >= new DateTime(2010, 05, 15)
					select user;
			Assert.Equal("Birthday:[20100515000000000 TO NULL]", q.ToString());
        }

        [Fact]
        public void CanUnderstandProjectionOfOneField()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
					where user.Birthday >= new DateTime(2010, 05, 15)
					select user.Name;
			Assert.Equal("<Name>: Birthday:[20100515000000000 TO NULL]", q.ToString());
        }

        [Fact]
        public void CanUnderstandProjectionOfMultipleFields()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
        	var dateTime = new DateTime(2010, 05, 15);
        	var q = from user in indexedUsers
					where user.Birthday >= dateTime
					select new { user.Name, user.Age };
			Assert.Equal("<Name, Age>: Birthday:[20100515000000000 TO NULL]", q.ToString());
        }

		[Fact]
		public void CanUnderstandSimpleEqualityOnInt()
		{
			var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
			var q = from user in indexedUsers
					where user.Age == 3
					select user;
			Assert.Equal("Age:3", q.ToString());
		}


		[Fact]
		public void CanUnderstandGreaterThanOnInt()
		{
			var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
			var q = from user in indexedUsers
					where user.Age > 3
					select user;
			Assert.Equal("Age_Range:{0x00000003 TO NULL}", q.ToString());
		}

        [Fact]
        public void CanUnderstandMethodCalls()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
            var q = from user in indexedUsers
                    where user.Birthday >= DateTime.Parse("2010-05-15")
                    select new { user.Name, user.Age };
			Assert.Equal("<Name, Age>: Birthday:[20100515000000000 TO NULL]", q.ToString());
        }

        [Fact]
        public void CanUnderstandConvertExpressions()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
            var q = from user in indexedUsers
                    where user.Age == Convert.ToInt16("3")
                    select user;
            Assert.Equal("Age:3", q.ToString());
        }

		[Fact]
		public void CanChainMultipleWhereClauses()
		{
			var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
			var q = indexedUsers
				.Where(x => x.Age == 3)
			    .Where(x => x.Name == "ayende");
			Assert.Equal("Age:3 AND Name:[[ayende]]", q.ToString());
		}

        [Fact]
        public void CanUnderstandSimpleAny()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null, null));
            var q = indexedUsers
                .Where(x => x.Properties.Any(y => y.Key == "first"));
            Assert.Equal("Key:[[first]]", q.ToString());
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
    }
}