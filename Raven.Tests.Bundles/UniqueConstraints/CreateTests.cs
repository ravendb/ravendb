using Raven.Abstractions.Connection;
using Raven.Abstractions.Exceptions;
using Raven.Json.Linq;

using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints
{
    public class CreateTests : UniqueConstraintsTest
	{
		[Fact]
		public void Will_create_correct_metadata()
		{
			var user = new User { Id = "users/1", Email = "foo@bar.com", Name = "James" };

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(user);
				session.SaveChanges();
			}

			using (var session = DocumentStore.OpenSession())
			{
				var loadedUser = session.Load<User>(user.Id);
				var userMetadata = session.Advanced.GetMetadataFor(loadedUser);
				var constraintsMeta = userMetadata.Value<RavenJArray>("Ensure-Unique-Constraints");
				Assert.NotNull(constraintsMeta);
				Assert.Equal(2, constraintsMeta.Length);
			}
		}

		[Fact]
		public void Will_create_documents_with_different_constraints()
		{
			var user = new User { Id = "users/1", Email = "foo@bar.com", Name = "James" };
			var otherUser = new User { Id = "users/2", Email = "bar@foo.com", Name = "John" };

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(user);
				session.Store(otherUser);
				session.SaveChanges();
			}

			Assert.NotNull(user.Id);
			Assert.NotNull(otherUser.Id);
		}

		[Fact]
		public void Will_create_constraint_document()
		{
			var user = new User { Email = "foo@bar.com", Name = "James" };

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(user);
				session.SaveChanges();
			}

			using (var session = DocumentStore.OpenSession())
			{
			    var key = Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("foo@bar.com");
                var constraintDocument = session.Load<dynamic>("UniqueConstraints/Users/Email/" + key);

				Assert.NotNull(constraintDocument);
				Assert.Equal(constraintDocument.Constraints[key].RelatedId, user.Id);
			}
		}

		[Fact]
		public void Will_veto_on_same_constraint()
		{
			var user = new User { Email = "foo@bar.com", Name = "Khan" };
			var sameEmailUser = new User { Email = "foo@bar.com", Name = "James" };

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(user);
				session.SaveChanges();
			}

			Assert.Throws<ErrorResponseException>(
				() =>
					{
						using (var session = DocumentStore.OpenSession())
						{
							session.Store(sameEmailUser);
							session.SaveChanges();
						}
					});
		}

		[Fact]
		public void Will_veto_on_same_constraint_same_tx()
		{
			var user = new User { Email = "foo@bar.com", Name = "Khan" };
			var sameEmailUser = new User { Email = "foo@bar.com", Name = "James" };

            Assert.Throws<ErrorResponseException>(
				() =>
				{
					using (var session = DocumentStore.OpenSession())
					{
						session.Store(user);
						session.Store(sameEmailUser);
						session.SaveChanges();
					}
				});
		}

	    [Fact]
	    public void Will_veto_on_same_constraint_array()
	    {
	        var user = new User {Email = "foo@bar.com", TaskIds = new[] {"TaskA", "TaskB"}};
            var sameTaskUser = new User { Email = "foo2@bar.com", TaskIds = new[] { "TaskA", "TaskC" } };

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(user);
                session.SaveChanges();
            }

            Assert.Throws<ErrorResponseException>(
                () =>
                {
                    using (var session = DocumentStore.OpenSession())
                    {
                        session.Store(sameTaskUser);
                        session.SaveChanges();
                    }
                });
	    }
	}
}
