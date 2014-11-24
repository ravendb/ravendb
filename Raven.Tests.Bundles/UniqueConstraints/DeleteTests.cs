using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints
{
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
					DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(("foo@bar.com"))));
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("users/1"));

				DocumentStore.DatabaseCommands.Delete("users/1", null);

				// Both docs should be deleted
				Assert.Null(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("foo@bar.com")));
				Assert.Null(DocumentStore.DatabaseCommands.Get("users/1"));
			}
		}

        [Fact]
        public void Deletes_array_constraint_documents_when_base_document_is_deleted()
        {
            var user = new User { Id = "users/1", Email = "foo@bar.com", Name = "James", TaskIds = new []{"Task1", "Task2"}};

            using (var session = DocumentStore.OpenSession())
            {
                session.Store(user);
                session.SaveChanges();

                // Ensures constraint was created
                Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/TaskIds/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(("Task1"))));
                Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/TaskIds/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(("Task2"))));
                Assert.NotNull(DocumentStore.DatabaseCommands.Get("users/1"));

                DocumentStore.DatabaseCommands.Delete("users/1", null);

                // Both docs should be deleted
                Assert.Null(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/TaskIds/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(("Task1"))));
                Assert.Null(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/TaskIds/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue(("Task2"))));
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
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("foo@bar.com")));
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("users/1"));

				DocumentStore.DatabaseCommands.Delete("UniqueConstraints/Users/Email/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("foo@bar.com"), null);

				// Base doc still intact
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("users/1"));
			}
		}
	}
}
