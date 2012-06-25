extern alias database;
namespace Raven.Bundles.Tests.UniqueConstraints
{
	using System;

	using Xunit;

	using RavenJArray = Raven.Json.Linq.RavenJArray;

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
				Assert.Equal(1, constraintsMeta.Length);
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
				var constraintDocument = session.Load<dynamic>("UniqueConstraints/Users/Email/" + Uri.EscapeDataString("foo@bar.com"));

				Assert.NotNull(constraintDocument);
				Assert.Equal(constraintDocument.RelatedId, user.Id);
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

			Assert.Throws<database::Raven.Database.Exceptions.OperationVetoedException>(
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

			Assert.Throws<database::Raven.Database.Exceptions.OperationVetoedException>(
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
	}
}
