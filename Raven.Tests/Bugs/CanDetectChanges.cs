//-----------------------------------------------------------------------
// <copyright file="CanDetectChanges.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Linq;
using Xunit;
using Raven.Client.Document;

namespace Raven.Tests.Bugs
{
	public class CanDetectChanges : RavenTest
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
					Assert.True(session.Advanced.HasChanges);
					session.SaveChanges();
					Assert.False(session.Advanced.HasChanges);
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
					Assert.False(session.Advanced.HasChanged(registration));
					Assert.False(session.Advanced.HasChanges);
					registration.RegisteredAt = new DateTime(2010, 2, 1);
					Assert.True(session.Advanced.HasChanges);
					Assert.True(session.Advanced.HasChanged(registration));
					session.SaveChanges();
					Assert.False(session.Advanced.HasChanges);
					Assert.False(session.Advanced.HasChanged(registration));
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
					var registration = session.Advanced.LuceneQuery<ProjectingDates.Registration>()
						.WaitForNonStaleResults()
						.Single();
					Assert.False(session.Advanced.HasChanges);
					Assert.False(session.Advanced.HasChanged(registration));
					registration.RegisteredAt = new DateTime(2010, 2, 1);
					Assert.True(session.Advanced.HasChanges);
					Assert.True(session.Advanced.HasChanged(registration));
					session.SaveChanges();
					Assert.False(session.Advanced.HasChanges);
					Assert.False(session.Advanced.HasChanged(registration));
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
						session.Advanced.LuceneQuery<ProjectingDates.Registration>().WaitForNonStaleResults().ToArray();

						session.SaveChanges();
					}
				}

				using (var session = store.OpenSession())
				{
					Assert.Equal(2, session.Advanced.LuceneQuery<ProjectingDates.Registration>().WaitForNonStaleResults().ToList().Count());
				}
			}
		}

		[Fact]
		public void CanDetectChangesOnExistingItem_ByteArray()
		{
			using (var server = GetNewServer())
			using (var store = new DocumentStore { Url = "http://localhost:8079" }.Initialize())
			{
				var id = string.Empty;
				using (var session = store.OpenSession())
				{
					var doc = new ByteArraySample
					{
						Bytes = Guid.NewGuid().ToByteArray(),
					};
					session.Store(doc);
					session.SaveChanges();
					id = doc.Id;
				}

				WaitForAllRequestsToComplete(server);
				server.Server.ResetNumberOfRequests();

				using (var session = store.OpenSession())
				{
					var sample = session.Load<ByteArraySample>(id);
					Assert.False(session.Advanced.HasChanged(sample));
					Assert.False(session.Advanced.HasChanges);
				}
			}
		}

		public class ByteArraySample
		{
			public string Id { get; set; }
			public byte[] Bytes { get; set; }
		}
	}
}
