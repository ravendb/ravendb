using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.Bundles.Replication.Issues
{
    public class RavenDB_11663: ReplicationBase
    {
        [Fact]
        public void CanReplicateDeletes()
        {
            var store1 = CreateStore(requestedStorageType:"esent");
            var store2 = CreateStore();
            var id1 = new string('a',240)+'1';
            var id2 = new string('a', 240)+'2';

            RunReplication(store1, store2);
            RunReplication(store2, store1);

            Etag etag;
            using (var session = store1.OpenSession())
            {
                var company = new Company {Name = "Hibernating Rhinos"};
                session.Store(company, id1);                
                company = new Company { Name = "Hibernating Rhinos 2" };
                session.Store(company, id2);
                session.SaveChanges();
            }
            
            WaitForReplication(store2, id2);
            using (var session = store1.OpenSession())
            {                
                session.Delete(id1);
                session.Delete(id2);
                var marker = new Company {Name = "Marker"};
                session.Store(marker,"marker");
                etag = session.Advanced.GetEtagFor(marker);
                session.SaveChanges();
            }

            WaitForReplication(store2, "marker", changedSince: etag);
            using (var session = store2.OpenSession())
            {
                Assert.Null(session.Load<Company>(id1));
                Assert.Null(session.Load<Company>(id2));
            }
        }
    }
}
