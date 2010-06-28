using System;
using Xunit;

namespace Raven.Client.Tests.Bugs
{
	public class CanDetectChanges : BaseClientTest
	{
		[Fact]
		public void CanDetectChangesOnNewItem()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new ProjectingDates.Registration
					{
						RegisteredAt = new DateTime(2010, 1, 1)
					});
					Assert.True(session.HasChanges);
					session.SaveChanges();
					Assert.False(session.HasChanges);
				}
			}
		}

		[Fact]
		public void CanDetectChangesOnExistingItem()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new ProjectingDates.Registration
					{
						RegisteredAt = new DateTime(2010, 1, 1)
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					var registration = session.Load<ProjectingDates.Registration>("registrations/1");
					Assert.False(session.HasChanges);
					Assert.False(session.HasChanged(registration));
					registration.RegisteredAt = new DateTime(2010, 2, 1);
					Assert.True(session.HasChanges);
					Assert.True(session.HasChanged(registration));
					session.SaveChanges();
					Assert.False(session.HasChanges);
					Assert.False(session.HasChanged(registration));
				}
			}
		}
	}
}