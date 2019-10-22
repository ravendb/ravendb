using System;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB9158 : RavenTestBase
    {
        public RavenDB9158(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }

            public string Username { get; set; }
        }

        [Fact]
        public void Raven_Should_Throw_Concurrency_Exception_When_Using_Loaded_Entity_And_Specific_Change_Vector()
        {
            using (var store = GetDocumentStore())
            {

                string changeVector;
                string id;

                // Save initial document in first session and get change vector
                using (var session = store.OpenSession())
                {
                    var user = new User()
                    {
                        Id = "Users/",
                        Username = "foo"
                    };

                    session.Store(user);
                    session.SaveChanges();

                    id = user.Id;
                    changeVector = session.Advanced.GetChangeVectorFor(user);
                }

                // simulate another session making a change
                using (var session = store.OpenSession())
                {
                    var user = new User()
                    {
                        Id = id,
                        Username = "bar"
                    };

                    session.Store(user);
                    session.SaveChanges();

                    var newChange = session.Advanced.GetChangeVectorFor(user);

                    Assert.NotEqual(changeVector, newChange);
                }

                using (var session = store.OpenSession())
                {

                    // load entity
                    var user = session.Load<User>(id);

                    // make a change
                    user.Username = "baz";

                    // should throw concurrency exception
                    // since we want to compare against changeVector
                    Assert.Throws(typeof(Raven.Client.Exceptions.ConcurrencyException), () =>
                    {
                        session.Store(user, changeVector, id);
                        session.SaveChanges();
                    });
                }
            }
        }

        [Fact]
        public void Raven_Should_Throw_Concurrency_Exception_When_Using_Loaded_Entity_From_Different_Session_And_Specific_Change_Vector()
        {
            using (var store = GetDocumentStore())
            {

                string changeVector;
                string id;

                // Save initial document in first session and get change vector
                using (var session = store.OpenSession())
                {
                    var user = new User()
                    {
                        Id = "Users/",
                        Username = "foo"
                    };

                    session.Store(user);
                    session.SaveChanges();

                    id = user.Id;
                    changeVector = session.Advanced.GetChangeVectorFor(user);
                }

                // simulate another session making a change
                using (var session = store.OpenSession())
                {
                    var user = new User()
                    {
                        Id = id,
                        Username = "bar"
                    };

                    session.Store(user);
                    session.SaveChanges();

                    var newChange = session.Advanced.GetChangeVectorFor(user);

                    Assert.NotEqual(changeVector, newChange);
                }

                Func<User> getUser = () =>
                {
                    using (var session = store.OpenSession())
                    {

                        // load entity
                        return session.Load<User>(id);
                    }
                };

                using (var session = store.OpenSession())
                {

                    // load entity from DIFFERENT session
                    var user = getUser();

                    // make a change
                    user.Username = "baz";

                    // should throw concurrency exception
                    // since we want to compare against changeVector
                    Assert.Throws(typeof(Raven.Client.Exceptions.ConcurrencyException), () =>
                    {
                        session.Store(user, changeVector, id);
                        session.SaveChanges();
                    });
                }
            }

        }
    }
}
