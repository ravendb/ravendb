// -----------------------------------------------------------------------
//  <copyright file="RavenDB1519.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB1519 : RavenTestBase
    {
        public RavenDB1519(ITestOutputHelper output) : base(output)
        {
        }

        private class Appointment
        {
            public string Id { get; set; }
        }

        private class Message
        {
            public string Id { get; set; }
        }

        private class Attachment
        {
            public string Id { get; set; }
            public string Source { get; set; }
        }

        private class Attachments_Unused : AbstractIndexCreationTask<Attachment>
        {
            public Attachments_Unused()
            {
                Map = attachments => from attachment in attachments
                                     where attachment.Source == null || LoadDocument<object>(attachment.Source) == null
                                     select new { attachment.Id };
            }
        }

        [Fact]
        public void IndexCompilationErr()
        {
            using (var store = GetDocumentStore())
            {
                new Attachments_Unused().Execute(store);

                using (var session = store.OpenSession())
                {
                    var appointment = new Appointment { Id = Guid.NewGuid().ToString() };
                    var message = new Message { Id = Guid.NewGuid().ToString() };

                    session.Store(appointment);
                    session.Store(message);

                    var appointmentAttachment = new Attachment
                    {
                        Id = Guid.NewGuid().ToString(),
                        Source = session.Advanced.GetDocumentId(appointment)
                    };

                    var messageAttachment = new Attachment
                    {
                        Id = Guid.NewGuid().ToString(),
                        Source = session.Advanced.GetDocumentId(message)
                    };

                    var unusedAttachment = new Attachment
                    {
                        Id = Guid.NewGuid().ToString(),
                        Source = null
                    };

                    session.Store(appointmentAttachment);
                    session.Store(messageAttachment);
                    session.Store(unusedAttachment);

                    session.SaveChanges();
                }


                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<Attachment, Attachments_Unused>().ToList());
                }

            }
        }
    }
}
