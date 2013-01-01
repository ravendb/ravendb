using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Bundles.Tests.UniqueConstraints
{
	using Raven.Client.UniqueConstraints;

	using Xunit;

	public class ExtensionCheckTests : UniqueConstraintsTest
	{
		[Fact]
		public void Will_load_all_documents_based_on_constraint()
		{
			var user1 = new User { Id = "users/1", Email = "user1@bar.com", Name = "James" };
			var user2 = new User { Id = "users/2", Email = "user2@bar.com", Name = "Watson" };
			var user3 = new User { Id = "users/3", Email = "user3@bar.com", Name = "Sherlock" };

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(user1);
				session.Store(user2);
				session.Store(user3);
				session.SaveChanges();
			}

			using (var session = DocumentStore.OpenSession())
			{
				var checkUser = new User { Id = "users/5", Email = "user2@bar.com", Name = "Watson" };

				var checkResult = session.CheckForUniqueConstraints(checkUser);

				Assert.Equal(checkResult.LoadedDocuments.Count(), 1);
				Assert.Equal(checkUser.Email, checkResult.DocumentForProperty(x => x.Email).Email);
			}
		}

		[Fact]
		public void Can_insert_document_when_constraints_are_free()
		{
			var user1 = new User { Id = "users/1", Email = "user1@bar.com", Name = "James" };
			var user2 = new User { Id = "users/2", Email = "user2@bar.com", Name = "Watson" };
			var user3 = new User { Id = "users/3", Email = "user3@bar.com", Name = "Sherlock" };

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(user1);
				session.Store(user2);
				session.Store(user3);
				session.SaveChanges();
			}

			using (var session = DocumentStore.OpenSession())
			{
				var checkUser = new User { Id = "users/5", Email = "user5@bar.com", Name = "McLovin" };

				var checkResult = session.CheckForUniqueConstraints(checkUser);

				Assert.True(checkResult.ConstraintsAreFree());

				session.Store(checkUser);
				Assert.DoesNotThrow(delegate
				{
					session.SaveChanges();
				});
			}
		}

		/* 
		 * Can't use OperationVetoedException here for some reason I don't understand 
		 * The code won't compile.
		 * 
		[Fact]
		public void Can_not_insert_document_when_constraints_are_used()
		{
			var user1 = new User { Id = "users/1", Email = "user1@bar.com", Name = "James" };
			var user2 = new User { Id = "users/2", Email = "user2@bar.com", Name = "Watson" };
			var user3 = new User { Id = "users/3", Email = "user3@bar.com", Name = "Sherlock" };

			using (var session = DocumentStore.OpenSession())
			{
				session.Store(user1);
				session.Store(user2);
				session.Store(user3);
				session.SaveChanges();
			}

			using (var session = DocumentStore.OpenSession())
			{
				var checkUser = new User { Id = "users/5", Email = "user2@bar.com", Name = "McLovin" };

				var checkResult = session.CheckForUniqueConstraints(checkUser);

				Assert.False(checkResult.ConstraintsAreFree());

				session.Store(checkUser);
				
				Assert.Throws<Raven.Database.Exceptions.OperationVetoedException>(delegate
				{
					session.SaveChanges();
				});
			}
		}*/
	}
}
