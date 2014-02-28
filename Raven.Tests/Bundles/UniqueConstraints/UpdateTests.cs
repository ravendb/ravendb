using Xunit;

namespace Raven.Tests.Bundles.UniqueConstraints
{
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
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("foo@bar.com")));
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("users/1"));

				user.Email = "bar@foo.com";
				session.SaveChanges();

				// Both docs should be deleted
				Assert.Null(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("foo@bar.com")));
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/Email/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("bar@foo.com")));
			}
		}

	    [Fact]
	    public void Updating_constraint_array_field_on_document_propagates_to_constraint_documents()
	    {
	        var user = new User {Id = "users/1", Email = "foo@bar.com", Name = "James", TaskIds = new []{"Task1", "Task2"}};

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(user);
				session.SaveChanges();

				// Ensures constraint was created
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/TaskIds/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("Task1")));
                Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/TaskIds/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("Task2")));
				Assert.NotNull(DocumentStore.DatabaseCommands.Get("users/1"));

			    user.TaskIds = new[] {"Task1", "Task3"};
	        
				session.SaveChanges();
                
                Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/TaskIds/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("Task1")));
                Assert.Null(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/TaskIds/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("Task2")));
                Assert.NotNull(DocumentStore.DatabaseCommands.Get("UniqueConstraints/Users/TaskIds/" + Raven.Bundles.UniqueConstraints.Util.EscapeUniqueValue("Task3")));
		    }
	    }
	}
}
