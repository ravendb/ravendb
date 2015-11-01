using System.Collections.Generic;
using Raven.Bundles.Replication.Tasks;
using Raven.Client;
using Raven.Json.Linq;
using Xunit;

using Raven.Client.UniqueConstraints;

namespace Raven.Tests.Bundles.UniqueConstraints
{
    public class ExtensionLoadTests : UniqueConstraintsTest
    {
        [Fact]
        public void Manipulate_UniqueConstraint_To_Look_LikeTheOldThenRunThis()
        {
            using (var session = DocumentStore.OpenSession())
            {
                var user = new User { Email = "foo@bar.com", Name = "James" };
                session.Store(user);

                session.SaveChanges();
            }

            var jsonDocument = DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Util.EscapeUniqueValue("foo@bar.com"));
            jsonDocument.DataAsJson = RavenJObject.Parse("{'RelatedId': 'users/1' }");
            DocumentStore.DatabaseCommands.Put(jsonDocument.Key, null, jsonDocument.DataAsJson, jsonDocument.Metadata);

            using (var session = DocumentStore.OpenSession())
            {
                var loadedUser = session.LoadByUniqueConstraint<User>(x => x.Email, "foo@bar.com");

                session.Delete(loadedUser);

                var user = new User { Email = "foo@bar.com", Name = "James" };
                session.Store(user);

                session.SaveChanges();
            }
        }

        [Fact]
        public void Will_load_existing_doc_by_constraint()
        {
            var user = new User { Id = "users/1", Email = "foo@bar.com", Name = "James" };

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(user);
                session.SaveChanges();
            }

            using (var session = DocumentStore.OpenSession())
            {
                var loadedUser = session.LoadByUniqueConstraint<User>(x => x.Email, "foo@bar.com");

                Assert.NotNull(loadedUser);
                Assert.Equal(user.Id,loadedUser.Id);
            }
        }

        [Fact]
        public void Will_return_null_when_there_is_no_constraint_doc()
        {
            var user = new User { Id = "users/1", Email = "foo@bar.com", Name = "James" };

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(user);
                session.SaveChanges();
            }

            using (var session = DocumentStore.OpenSession())
            {
                var loadedUser = session.LoadByUniqueConstraint<User>(x => x.Email, "bar@foo.com");

                Assert.Null(loadedUser);
            }
        }

        [Fact]
        public void Will_load_with_generic_property()
        {
            var namedValue = new GenericNamedValue<Dictionary<string, string>>
            {
                Id = "genericnamedvalue/1",
                Name = "asdf",
                Value = new Dictionary<string, string>()
            };

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(namedValue);
                session.SaveChanges();
            }

            using (var session = DocumentStore.OpenSession())
            {
                var loadedNamedValue = session.LoadByUniqueConstraint<GenericNamedValue<Dictionary<string, string>>>(x => x.Name, "asdf");

                Assert.NotNull(loadedNamedValue);
                Assert.Equal(namedValue.Id, loadedNamedValue.Id);
            }
        }

        [Fact]
        public void Will_multiload_with_array_of_values()
        {
            var user1 = new User { Id = "users/1", Email = "foo1@bar.com", Name = "James" };
            var user2 = new User { Id = "users/2", Email = "foo2@bar.com", Name = "James" };

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(user1);
                session.Store(user2);
                session.SaveChanges();
            }

            using (var session = DocumentStore.OpenSession())
            {
                var results = session.LoadByUniqueConstraint<User>(u => u.Email, "foo1@bar.com", "foo2@bar.com");
                Assert.Equal(2, results.Length);
            }
        }

        [Fact]
        public void Will_return_parallel_array_with_array_of_values()
        {
            var user1 = new User { Id = "users/1", Email = "foo1@bar.com", Name = "James" };
            var user3 = new User { Id = "users/3", Email = "foo3@bar.com", Name = "James" };

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(user1);
                session.Store(user3);
                session.SaveChanges();
            }

            using (var session = DocumentStore.OpenSession())
            {
                var results = session.LoadByUniqueConstraint<User>(u => u.Email, "foo1@bar.com", "foo2@bar.com", "foo3@bar.com");
                Assert.Equal(user1.Email, results[0].Email);
                Assert.Equal(null, results[1]);
                Assert.Equal(user3.Email, results[2].Email);
            }
        }

        [Fact]
        public void Will_load_existing_doc_by_constraint_with_property_name()
        {
            var user = new User { Id = "users/1", Email = "foo@bar.com", Name = "James" };

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(user);
                session.SaveChanges();
            }

            using (var session = DocumentStore.OpenSession())
            {
                var loadedUser = session.LoadByUniqueConstraint<User>("Email", "foo@bar.com");

                Assert.NotNull(loadedUser);
                Assert.Equal(user.Id, loadedUser.Id);
            }
        }
    }
}
