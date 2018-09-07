using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11902 : RavenTestBase
    {
        [Fact]
        public void Can_insert_doc_with_single_quotation_char_in_id()
        {
            using (var store = GetDocumentStore())
            {
                // this works
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = "GeoData/D'horn/113736"
                    });

                    session.SaveChanges();
                }

                // this works
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = "GeoData/D\"horn/113736"
                    });

                    session.SaveChanges();
                }

                // this fails
                using (var bulk = store.BulkInsert())
                {
                    bulk.Store(new User
                    {
                        Id = "GeoData/D'horn/113736"
                    });
                }

                // this fails
                using (var bulk = store.BulkInsert())
                {
                    bulk.Store(new User
                    {
                        Id = "GeoData/D\"horn/113736"
                    });
                }
            }
        }
    }
}
