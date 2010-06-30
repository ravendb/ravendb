using System;
using System.Linq;
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

		[Fact]
		public void CanDetectChangesOnExistingItemFromQuery()
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
					var registration = session.LuceneQuery<ProjectingDates.Registration>()
						.WaitForNonStaleResults()
						.Single();
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

		[Fact]
		public void WillNotCreateNewDocuments()
		{
			using (var store = NewDocumentStore())
			{
				using (var session = store.OpenSession())
				{
					session.Store(new ProjectingDates.Registration
					{
						RegisteredAt = new DateTime(2010, 1, 1)
					});
					session.Store(new ProjectingDates.Registration
					{
						RegisteredAt = new DateTime(2010, 1, 1)
					});
					session.SaveChanges();
				}

				using (var session = store.OpenSession())
				{
					for (int i = 0; i < 15; i++)
					{
						session.LuceneQuery<ProjectingDates.Registration>().WaitForNonStaleResults().ToArray();

						session.SaveChanges();
					}
				}

				using (var session = store.OpenSession())
				{
					Assert.Equal(2, session.LuceneQuery<ProjectingDates.Registration>().WaitForNonStaleResults().Count());
				}
			}
		}
	}
}