using System;
using Raven.Bundles.UniqueConstraints;

namespace Raven.Bundles.Tests.UniqueConstraints
{
	using Xunit;

	public class UpdateTests : UniqueConstraintsTest
	{
		[Fact]
		public void Updating_constraint_field_on_document_propagates_to_constraint_document()
		{
			var user = new User { Id = "users/1", Email = "foo@bar.com", Name = "James" };

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(user);
				session.SaveChanges();

				// Ensures constraint was created
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Util.EscapeUniqueValue("foo@bar.com")));
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("users/1"));

				user.Email = "bar@foo.com";
				session.SaveChanges();

				// Both docs should be deleted
				Assert.Null(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Util.EscapeUniqueValue("foo@bar.com")));
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Util.EscapeUniqueValue("bar@foo.com")));
			}
		}
	}
}
