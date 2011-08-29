using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Document;
using Xunit;

namespace Raven.Tests.Bugs
{
    public class ConflictsWithIIS : IISExpressTestClient
    {
        [Fact]
        public void MultiThreadedInsert()
        {
            const int threadCount = 4;
            var tasks = new List<Task>();

            using (var store = NewDocumentStore())
            {
                for (int i = 1; i <= threadCount; i++)
                {
                    var taskHandle = Task.Factory.StartNew(() => DoInsert(store, i));
                    tasks.Add(taskHandle);
                }

                Task.WaitAll(tasks.ToArray());
            }
        }

        [Fact]
        public void InnefficientMultiThreadedInsert()
        {
            const int threadCount = 4;
            var tasks = new List<Task>();

            using (var store = NewDocumentStore())
            {
                for (int i = 1; i <= threadCount; i++)
                {
                    var taskHandle = Task.Factory.StartNew(() => DoInefficientInsert(store.Url, i));
                    tasks.Add(taskHandle);
                }

                Task.WaitAll(tasks.ToArray());
            }
        }

        private void DoInsert(IDocumentStore store, int deviceId)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new
                                  {
                                      DeviceId = deviceId,
                                      Timestamp = DateTime.Now,
                                      StatusId = 1024
                                  });
                session.SaveChanges();
            }
        }

        private void DoInefficientInsert(string url, int deviceId)
        {
            using (var store = new DocumentStore {Url = url}.Initialize())
            using (var session = store.OpenSession())
            {
                session.Store(new
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
