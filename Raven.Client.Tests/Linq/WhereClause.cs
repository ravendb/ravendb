using Raven.Client.Linq;
using Xunit;
using System.Linq;

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
            Assert.Equal("Name:ayende ", q.ToString());
        }

        [Fact]
        public void CanUnderstandAnd()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
                    where user.Name == "ayende" && user.Email == "ayende@ayende.com"
                    select user;
            Assert.Equal("Name:ayende AND Email:ayende@ayende.com ", q.ToString());
        }

        [Fact]
        public void CanUnderstandOr()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
                    where user.Name == "ayende" || user.Email == "ayende@ayende.com"
                    select user;
            Assert.Equal("Name:ayende OR Email:ayende@ayende.com ", q.ToString());
        }

        [Fact]
        public void CanUnderstandLessThan()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
                    where user.Age < 10
                    select user;
            Assert.Equal("Age:[* TO 10] ", q.ToString());
        }

        [Fact]
        public void CanUnderstandLessThanOrEqualsTo()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
                    where user.Age <= 10
                    select user;
            Assert.Equal("Age:{* TO 10} ", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThan()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
                    where user.Age > 10
                    select user;
            Assert.Equal("Age:[10 TO *] ", q.ToString());
        }

        [Fact]
        public void CanUnderstandGreaterThanOrEqualsTo()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
                    where user.Age >= 10
                    select user;
            Assert.Equal("Age:{10 TO *} ", q.ToString());
        }

        [Fact]
        public void CanUnderstandProjectionOfOneField()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
                    where user.Age >= 10
                    select user.Name;
            Assert.Equal("<Name>: Age:{10 TO *} ", q.ToString());
        }

        [Fact]
        public void CanUnderstandProjectionOfMultipleFields()
        {
            var indexedUsers = new RavenQueryable<IndexedUser>(new RavenQueryProvider<IndexedUser>(null,null));
            var q = from user in indexedUsers
                    where user.Age >= 10
                    select new { user.Name , user.Age};
            Assert.Equal("<Name, Age>: Age:{10 TO *} ", q.ToString());
        }

        public class IndexedUser
        {
            public int Age { get; set; }
            public string Name { get; set; }
            public string Email { get; set; }
        }
    }
}