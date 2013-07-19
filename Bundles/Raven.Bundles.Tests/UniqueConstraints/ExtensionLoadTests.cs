using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Bundles.Tests.UniqueConstraints
{
	using Xunit;

	using Raven.Client.UniqueConstraints;

	public class ExtensionLoadTests : UniqueConstraintsTest
	{
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
    }
}
