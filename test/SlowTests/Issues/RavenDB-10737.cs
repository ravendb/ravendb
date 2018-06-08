using System;
using FastTests;
using Raven.Client;
using Sparrow;
using Sparrow.Extensions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10737: RavenTestBase
    {
        private class SampleItem
        {
            public string Id { get; set; }
        }

        private const string _expires = Constants.Documents.Metadata.Expires;

        [Fact]
        public void Insert_with_Expirations()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var expirationDate = DateTime.UtcNow.AddDays(2);

                    var item = new SampleItem();
                    session.Store(item);
                    session.Advanced.GetMetadataFor(item)[_expires] = expirationDate;
                    session.SaveChanges();

                    Assert.Equal(expirationDate.GetDefaultRavenFormat(true),
                                 session.Advanced.GetMetadataFor(item)[_expires]);
                }
            }
        }

        [Fact]
        public void Insert_then_Load_and_Update_with_Expirations()
        {
            using (var store = GetDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var item = new SampleItem();
                    session.Store(item);
                    session.SaveChanges();
                    id = item.Id;
                }

                using (var session = store.OpenSession())
                {
                    var expirationDate = DateTime.UtcNow.AddDays(2);

                    var item = session.Load<SampleItem>(id);
                    session.Advanced.GetMetadataFor(item)[_expires] = expirationDate;
                    session.SaveChanges();

                    Assert.Equal(expirationDate.GetDefaultRavenFormat(true), 
                                 session.Advanced.GetMetadataFor(item)[_expires]);
                }
            }
        }


        [Fact]
        public void Insert_then_Load_and_Update_with_Expirations_DateTimeOffset()
        {
            using (var store = GetDocumentStore())
            {
                string id;
                using (var session = store.OpenSession())
                {
                    var item = new SampleItem();
                    session.Store(item);
                    session.SaveChanges();
                    id = item.Id;
                }

                using (var session = store.OpenSession())
                {
                    var expirationDate = DateTimeOffset.UtcNow.AddDays(2);

                    var item = session.Load<SampleItem>(id);
                    session.Advanced.GetMetadataFor(item)[_expires] = expirationDate;
                    session.SaveChanges();

                    Assert.Equal(expirationDate.ToString(DefaultFormat.DateTimeOffsetFormatsToWrite),
                                 session.Advanced.GetMetadataFor(item)[_expires]);
                }
            }
        }
    }
}
