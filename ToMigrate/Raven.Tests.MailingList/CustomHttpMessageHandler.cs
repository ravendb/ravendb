using System;
using System.Net.Http;
using Raven.Client.Document;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.MailingList
{
    public class CustomHttpMessageHandler : RavenTest
    {
        private class Doc
        {
            public string Id { get; set; }
        }

        [Fact]
        public void CanSetHandler()
        {
            Action<DocumentStore> setHandler = store =>
            {
                store.HttpMessageHandlerFactory = () => new WebRequestHandler
                {
                    UnsafeAuthenticatedConnectionSharing = true,
                    ReadWriteTimeout = 5 * 60 * 1000
                };
            };

            using (var store = NewRemoteDocumentStore(configureStore: setHandler))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Doc { Id = "ayende" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Doc>("ayende");
                    Assert.NotNull(doc);
                }
            }
        }
    }
}
