using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Tests.Common.Attributes;
using Raven.Tests.Common.Util;

namespace Raven.Tests.Bugs.Async
{
    // related to issue RavenDB-2082 Trying to use sync API inside Web API hangs
    public class WebApi : IisExpressTestClient
    {
        public class DeviceStatusRecord
        {
            public int DeviceId { get; set; }
            public DateTimeOffset Timestamp { get; set; }
            public int StatusId { get; set; }
        }

        [IISExpressInstalledFact]
        public void InsertAndReadFromDB()
        {
            const int threadCount = 4;
            var tasks = new List<Task>();

            using (var store = NewDocumentStore())
            {
                DoInsert(store, 1);

                store.DatabaseCommands.GetDocuments(0, 50, true);
            }
        }


        private void DoInsert(IDocumentStore store, int deviceId)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new ConflictsWithIIS.DeviceStatusRecord
                {
                    DeviceId = deviceId,
                    Timestamp = DateTime.Now,
                    StatusId = 1024
                });
                session.SaveChanges();
            }
        }
        
    }
}
