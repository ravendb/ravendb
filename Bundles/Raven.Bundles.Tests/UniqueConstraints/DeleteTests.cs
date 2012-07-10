using System;
using Raven.Bundles.UniqueConstraints;

namespace Raven.Bundles.Tests.UniqueConstraints
{
	using Xunit;

	public class DeleteTests : UniqueConstraintsTest
	{
		[Fact]
		public void Deletes_constraint_document_when_base_document_is_deleted()
		{
			var user = new User { Id = "users/1", Email = "foo@bar.com", Name = "James" };

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(user);
				session.SaveChanges();

				// Ensures constraint was created
				Assert.NotNull(
					DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Util.EscapeUniqueValue(("foo@bar.com"))));
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("users/1"));

				DocumentStore.DatabaseCommands.Delete("users/1", null);

				// Both docs should be deleted
				Assert.Null(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Util.EscapeUniqueValue("foo@bar.com")));
				Assert.Null(DocumentStore.DatabaseCommands.Get("users/1"));
			}
		}

		[Fact]
		public void Does_not_delete_base_document_when_constraint_is_deleted()
		{
			var user = new User { Id = "users/1", Email = "foo@bar.com", Name = "James" };

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(user);
				session.SaveChanges();

				// Ensures constraint was created
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Util.EscapeUniqueValue("foo@bar.com")));
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("users/1"));

				DocumentStore.DatabaseCommands.Delete("UniqueConstraints/Users/Email/" + Util.EscapeUniqueValue("foo@bar.com"), null);

				// Base doc still intact
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("users/1"));
			}
		}
	}
}
