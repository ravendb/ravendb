// -----------------------------------------------------------------------
//  <copyright file="RavenDB-6754.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_6754 : RavenTest
    {
        [Fact]
        public void PatchShouldFailQuicklyForConcurrency()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var ticket1 = new Ticket { Id = "Ticket-1", Status = StatusCodes.Done };
                using (var session = store.OpenSession())
                {
                    session.Store(ticket1);
                    session.SaveChanges();
                }

                // Try to patch with failing concurrency (preval) check
                Assert.Throws<ConcurrencyException>(() =>
                {
                    store.DatabaseCommands.Patch(
                        ticket1.Id,
                        new[]
                        {
                            new PatchRequest
                            {
                                Type = PatchCommandType.Set,
                                Name = "Status",
                                Value = StatusCodes.InProcess,
                                PrevVal = StatusCodes.Scheduled
                            }
                        });
                });
            }
        }
        [Fact]
        public void MultiplePatchShouldFailForConcurrency()
        {
            using (var store = NewRemoteDocumentStore())
            {
                var ticket1 = new Ticket { Id = "Ticket-1", Status = StatusCodes.Done };
                using (var session = store.OpenSession())
                {
                    session.Store(ticket1);
                    session.SaveChanges();
                }

                // Try to patch with failing concurrency (preval) check
                Parallel.For((long)0, 3, index =>
                {
                    Assert.Throws<ConcurrencyException>(() =>
                    {
                        store.DatabaseCommands.Patch(
                            ticket1.Id,
                            new[]
                            {
                                new PatchRequest
                                {
                                    Type = PatchCommandType.Set,
                                    Name = "Status",
                                    Value = StatusCodes.InProcess,
                                    PrevVal = StatusCodes.Scheduled
                                }
                            });
                    });
                });
            }
        }

        public class StatusCodes
        {
            public const string Scheduled = "Scheduled";
            public const string InProcess = "InProcess";
            public const string Done = "Done";
        }

        public class Ticket
        {
            public string Id { get; set; }
            public string Status { get; set; }
        }
    }
}
