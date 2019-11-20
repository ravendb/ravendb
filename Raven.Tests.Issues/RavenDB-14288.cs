using Raven.Client.Listeners;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_14288 : RavenTest
    {
        [Fact]
        public void IDocumentConversionListener_runs_only_once()
        {
            using (var store = NewDocumentStore())
            {
                var listener = new ConvertionListener();
                store.RegisterListener(listener);

                using (var session = store.OpenSession())
                {
                    var account = new User
                    {
                        Name = "Grisha"
                    };

                    session.Store(account);
                    session.SaveChanges();
                }

                Assert.Equal(1, listener.NumberOfCallsToBeforeConversionToDocument);
                Assert.Equal(1, listener.NumberOfCallsToAfterConversionToDocument);
            }
        }

        public class ConvertionListener : IDocumentConversionListener
        {
            public int NumberOfCallsToBeforeConversionToDocument;
            public int NumberOfCallsToAfterConversionToDocument;

            public void BeforeConversionToDocument(string key, object entity, RavenJObject metadata)
            {
                NumberOfCallsToBeforeConversionToDocument++;
            }

            public void AfterConversionToDocument(string key, object entity, RavenJObject document, RavenJObject metadata)
            {
                NumberOfCallsToAfterConversionToDocument++;
            }

            public void BeforeConversionToEntity(string key, RavenJObject document, RavenJObject metadata)
            {
            }

            public void AfterConversionToEntity(string key, RavenJObject document, RavenJObject metadata, object entity)
            {
            }
        }
    }
}
