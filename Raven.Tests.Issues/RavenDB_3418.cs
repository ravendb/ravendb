using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Tests.Common;
using Xunit;
using Xunit.Extensions;

namespace Raven.Tests.Issues
{
    public class RavenDB_3418 : RavenTest
    {
        /// <summary>
        /// If you broke this test, it is probably because you have changed the format of the compact messages.
        /// Please change the code in the admin controllers for both databases and filesystem accordingly.
        /// </summary>
        /// <param name="requestedStorage"></param>
        [Theory]
        [PropertyData("Storages")]
        public void CompactUpdateMessagesShouldNotAppearInMessages(string requestedStorage)
        {
            using (var documentStore = NewRemoteDocumentStore(requestedStorage: requestedStorage, databaseName:"Test",runInMemory:false))
            {
                using (var session = documentStore.OpenSession())
                {
                    session.Store(new RavenJObject());
                    session.SaveChanges();
                }
                var ravenHtttpFactory = new HttpRavenRequestFactory();
                var request = ravenHtttpFactory.Create("http://localhost:8079/admin/compact?database=Test",HttpMethod.Post, new RavenConnectionStringOptions());
                var response = request.ExecuteRequest<RavenJObject>();
                using (var systemDocumentStore = new DocumentStore() { Url = "http://localhost:8079" }.Initialize())
                {
                    using (var session = systemDocumentStore.OpenSession())
                    {
                        var stopeWatch = new Stopwatch();
                        stopeWatch.Start();
                        do
                        {
                            if (stopeWatch.Elapsed >= timeout) throw new TimeoutException("Waited to long for test to complete compaction.");
                            var statusRequest = ravenHtttpFactory.Create("http://localhost:8079/operation/status?id=" + response.Value<string>("OperationId"), HttpMethod.Get, new RavenConnectionStringOptions());
                            var status = statusRequest.ExecuteRequest<RavenJObject>();
                            if (status.Value<bool>("Completed"))
                            {
                                var compactStatus = session.Load<CompactStatus>(CompactStatus.RavenDatabaseCompactStatusDocumentKey("Test"));
                                Assert.Equal(compactStatus.Messages.Count, storageToExpectedLength[requestedStorage]);
                                return;
                            } else if (status.Value<bool>("Faulted"))
                            {
                                throw new Exception("Something went wrong, compaction was not successful");
                            }
                            Thread.Sleep(1000);
                            
                        } while (true);
                    }
                }
            }
            
        }

        private static TimeSpan timeout = TimeSpan.FromSeconds(120);
        private static Dictionary<string,int> storageToExpectedLength = new Dictionary<string, int>(){{"voron",5},{"esent",3}}; 
    }
}
