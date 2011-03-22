using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using Raven.Client.Client;
using Xunit;

namespace Raven.Tests.Bugs.Queries
{
    public class WhereTester : LocalClientTest
    {
        [Fact]
        public void CanUnderstandSimpleContainsWithClauses()
        {
            using (EmbeddableDocumentStore documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    var user1 = new User {Id = @"user\222", Name = "John Doe"};
                    session.Store(user1);
                    var user2 = new User {Id = @"user\444", Name = "Jane Travolta"};
                    session.Store(user2);
                    
                    session.SaveChanges();
                }
                using (var session = documentStore.OpenSession())
                {
                    var q = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.Name.Contains("John"))
                            .SingleOrDefault();

                    Assert.NotNull(q);
                }
            }   
        }

        [Fact]
        public void CanUnderstandSimpleStartsWithClauses()
        {
            using (EmbeddableDocumentStore documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    var user1 = new User { Id = @"user\222", Name = "John Doe" };
                    session.Store(user1);
                    var user2 = new User { Id = @"user\444", Name = "Jane Travolta" };
                    session.Store(user2);

                    session.SaveChanges();
                }
                using (var session = documentStore.OpenSession())
                {
                    var q = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(x => x.Name.StartsWith("John"))
                            .SingleOrDefault();

                    Assert.NotNull(q);
                }
            }
        }

        [Fact]
        public void CanUnderstandSimpleContainsInExpresssion1()
        {
            Func<User, bool> where = x => x.Name.Contains("John*");
            using (EmbeddableDocumentStore documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    var user1 = new User { Id = @"user\222", Name = "John Doe" };
                    session.Store(user1);
                    var user2 = new User { Id = @"user\444", Name = "Jane Travolta" };
                    session.Store(user2);

                    session.SaveChanges();
                }
                using (var session = documentStore.OpenSession())
                {
                    var q = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(where)
                            .ToArray();

                    Assert.NotNull(q);
                    Assert.True(q.Length == 1);
                }
            }   
        }

        [Fact]
        public void CanUnderstandSimpleContainsInExpresssion2()
        {
            Expression<Func<User, bool>> where = x => x.Name.Contains("John*");

            using (EmbeddableDocumentStore documentStore = NewDocumentStore())
            {
                using (var session = documentStore.OpenSession())
                {
                    var user1 = new User { Id = @"user\222", Name = "John Doe" };
                    session.Store(user1);
                    var user2 = new User { Id = @"user\444", Name = "Jane Travolta" };
                    session.Store(user2);

                    session.SaveChanges();
                }
                using (var session = documentStore.OpenSession())
                {
                    var q = session.Query<User>()
                            .Customize(x => x.WaitForNonStaleResults())
                            .Where(where)
                            .ToArray();

                    Assert.NotNull(q);
                    Assert.True(q.Length == 1);
                }
            }
        }

        //[Fact]
        //public void CanUnderstandSimpleStartsWithInExpresssion1()
        //{
        //    Func<User, bool> where = x => x.Name.StartsWith("ayende");

        //    var Users = GetRavenQueryInspector();
        //    var q = Users.Where(where).SingleOrDefault();

        //    Assert.NotNull(q);
        //    Assert.Equal("Name:ayende", q.ToString());
        //}


        //[Fact]
        //public void CanUnderstandSimpleStartsWithInExpresssion2()
        //{
        //    Expression<Func<User, bool>> where = x => x.Name.StartsWith("ayende");

        //    var Users = GetRavenQueryInspector();
        //    var q = Users.Where(where).SingleOrDefault();

        //    Assert.NotNull(q);
        //    Assert.Equal("Name:ayende", q.ToString());
        //}
    }
}
