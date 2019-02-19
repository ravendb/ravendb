using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Xunit;

namespace SlowTests.Bugs
{
    public class ConflictsWithRemote: RavenTestBase
    {
        private class DeviceStatusRecord
        {
            public int DeviceId { get; set; }
            public DateTimeOffset Timestamp { get; set; }
            public int StatusId { get; set; }
        }

        [Fact]
        public void MultiThreadedInsert()
        {
            const int threadCount = 4;
            var tasks = new List<Task>();

            DoNotReuseServer();
            using (var store = GetDocumentStore())
            {
                for (int i = 1; i <= threadCount; i++)
                {
                    var copy = i;
                    var taskHandle = Task.Factory.StartNew(() => DoInsert(store, copy));
                    tasks.Add(taskHandle);
                }

                Task.WaitAll(tasks.ToArray());
            }
        }

        [Fact]
        public void InnefficientMultiThreadedInsert()
        {
            DoNotReuseServer();
            const int threadCount = 4;
            var tasks = new List<Task>();
            for (int i = 1; i <= threadCount; i++)
            {
                var copy = i;
                var taskHandle = Task.Factory.StartNew(() => DoInefficientInsert(copy));
                tasks.Add(taskHandle);
            }

            Task.WaitAll(tasks.ToArray());
            
        }

        private void DoInsert(IDocumentStore store, int deviceId)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new DeviceStatusRecord
                {
                    DeviceId = deviceId,
                    Timestamp = DateTime.Now,
                    StatusId = 1024
                });
                session.SaveChanges();
            }
        }

        private void DoInefficientInsert(int deviceId)
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new DeviceStatusRecord
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
}
