using System;
using Xunit;
using Constants = Raven.Abstractions.Data.Constants;

namespace Raven.Tests.Bugs
{
	public class ReadOnly : RavenTest
	{
		[Fact]
		public void CanMarkEntityAsReadOnly()
		{
			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					var entity = new User {Name = "Ayende"};
					session.Store(entity);
					session.Advanced.GetMetadataFor(entity)[Constants.RavenReadOnly] = true;
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var user = session.Load<User>("users/1");
					user.Name = "Oren";
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var user = session.Load<User>("users/1");
					Assert.Equal("Ayende", user.Name);
				}
			}
		}

		[Fact]
		public void ReadOnlyPreventsDeletes()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var entity = new User { Name = "Ayende" };
					session.Store(entity);
					session.Advanced.GetMetadataFor(entity)[Constants.RavenReadOnly] = true;
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var user = session.Load<User>("users/1");
					Assert.Throws<InvalidOperationException>(() => session.Delete(user));
				}
			}
		}

		[Fact]
		public void CanRemoveReadOnlyMarker()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var entity = new User { Name = "Ayende" };
					session.Store(entity);
					session.Advanced.GetMetadataFor(entity)[Constants.RavenReadOnly] = true;
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var user = session.Load<User>("users/1");
					user.Name = "Oren";
					session.Advanced.GetMetadataFor(user)[Constants.RavenReadOnly] = false;
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var user = session.Load<User>("users/1");
					Assert.Equal("Oren", user.Name);
				}
			}
		}
	}
}