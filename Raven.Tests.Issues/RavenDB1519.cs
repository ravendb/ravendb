// -----------------------------------------------------------------------
//  <copyright file="RavenDB1519.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB1519 : RavenTest
    {
        public class Appointment
        {
            public Guid Id { get; set; }
        }

        public class Message
        {
            public Guid Id { get; set; }
        }

        public class Attachment
        {
            public Guid Id { get; set; }
            public string Source { get; set; }
        }

        public class Attachments_Unused : AbstractIndexCreationTask<Attachment>
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
            using (var store = NewDocumentStore())
            {
                new Attachments_Unused().Execute(store);

                using (var session = store.OpenSession())
                {
                    var appointment = new Appointment { Id = Guid.NewGuid() };
                    var message = new Message { Id = Guid.NewGuid() };

                    session.Store(appointment);
                    session.Store(message);

                    var appointmentAttachment = new Attachment
                    {
                        Id = Guid.NewGuid(),
                        Source = session.Advanced.GetDocumentId(appointment)
                    };

                    var messageAttachment = new Attachment
                    {
                        Id = Guid.NewGuid(),
                        Source = session.Advanced.GetDocumentId(message)
                    };

                    var unusedAttachment = new Attachment
                    {
                        Id = Guid.NewGuid(),
                        Source = null
                    };

                    session.Store(appointmentAttachment);
                    session.Store(messageAttachment);
                    session.Store(unusedAttachment);

                    session.SaveChanges();
                }


                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<Attachment, Attachments_Unused>().ToList());
                }
                   
            }
        }
    }
}