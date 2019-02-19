using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11191 : RavenTestBase
    {
        [Fact]
        public void NullableEnumWithSaveEnumAsInt()
        {
            using (var store = GetDocumentStore())
            {
                new DeviceIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Device
                    {
                        Name = "Phone",
                        History = new[]
                        {
                            new DeviceHistory
                            {
                                Timestamp = DateTime.UtcNow.AddHours(-1.0),
                                Status = DeviceStatus.Lost
                            },
                            new DeviceHistory
                            {
                                Timestamp = DateTime.UtcNow,
                                Status = DeviceStatus.Found
                            }
                        }
                    });

                    session.SaveChanges();
                }
            }
        }

        class DeviceIndex : AbstractIndexCreationTask<Device>
        {
            public DeviceIndex()
            {
                Map = devices =>
                    from device in devices
                    let lastHistory = device.History.OrderByDescending(x => x.Timestamp).FirstOrDefault()
                    select new
                    {
                        device.Name,
                        Status = lastHistory != null ? (DeviceStatus?)lastHistory.Status : null
                    };
            }
        }

        class Device
        {
            public string Name { get; set; }
            public DeviceHistory[] History { get; set; }
        }

        class DeviceHistory
        {
            public DateTime Timestamp { get; set; }
            public DeviceStatus Status { get; set; }
        }

        enum DeviceStatus
        {
            Broken,
            Fixed,
            Lost,
            Found
        }
    }


}
