using System;
using Raven.Abstractions.Exceptions;
using Xunit;

namespace Raven.Tests.Bugs
{
	public class OfflineConcurrency : RavenTest
	{
		[Fact]
		public void Successful()
		{
			Guid guid;

			using(var store = NewDocumentStore())
			{
				using(var session = store.OpenSession())
				{
					var entity = new User{Id = "users/1"};
					session.Store(entity);
					session.SaveChanges();

					guid = session.Advanced.GetEtagFor(entity).Value;
				}

				using (var session = store.OpenSession())
				{
					session.Advanced.UseOptimisticConcurrency = true; 
					var entity = new User { Id = "users/1" };
					session.Store(entity, guid);
					session.SaveChanges();
				}
			}
		}

		[Fact]
		public void Failed()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					var entity = new User { Id = "users/1" }; session.Store(entity);
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					session.Advanced.UseOptimisticConcurrency = true;
					var entity = new User { Id = "users/1" }; 
					session.Store(entity, Guid.NewGuid());
					Assert.Throws<ConcurrencyException>(() => session.SaveChanges());
				}
			}
		}
	}
}