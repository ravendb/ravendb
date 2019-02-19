//-----------------------------------------------------------------------
// <copyright file="CanDetectChanges.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using FastTests;
using Xunit;

namespace SlowTests.Bugs
{
    public class CanDetectChanges : RavenTestBase
    {
        [Fact]
        public void CanDetectChangesOnNewItem()
        {
            using (var store = GetDocumentStore())
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
            using (var store = GetDocumentStore())
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
                    var registration = session.Load<ProjectingDates.Registration>("registrations/1-A");
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
            using (var store = GetDocumentStore())
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
                    var registration = session.Advanced.DocumentQuery<ProjectingDates.Registration>()
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
            using (var store = GetDocumentStore())
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
                        session.Advanced.DocumentQuery<ProjectingDates.Registration>().WaitForNonStaleResults();

                        session.SaveChanges();
                    }
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.DocumentQuery<ProjectingDates.Registration>().WaitForNonStaleResults();
                    Assert.Equal(2, session.Advanced.DocumentQuery<ProjectingDates.Registration>().CountLazily().Value);
                }
            }
        }

        [Fact]
        public void CanDetectChangesOnExistingItem_ByteArray()
        {
            using (var store = GetDocumentStore())
            {
                var id = string.Empty;
                using (var session = store.OpenSession())
                {
                    session.Advanced.WaitForIndexesAfterSaveChanges();
                    var doc = new ByteArraySample
                    {
                        Bytes = Guid.NewGuid().ToByteArray(),
                    };
                    session.Store(doc);
                    session.SaveChanges();
                    id = doc.Id;
                }
                
                Server.Metrics.Reset();
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
