using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15097 : RavenTestBase
    {
        public RavenDB_15097(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task TimeSeriesMultiMapReduceShouldWork()
        {
            var deviceId = "device/1";

            using var store = GetDocumentStore();
            await new DeviceInfoIndexMapReduce().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Device { Id = deviceId });
                var baseline = DateTime.Now;
                var ts1 = session.TimeSeriesFor(deviceId, Device.Ts1);
                ts1.Append(baseline, new[] { 1d });

                var ts2 = session.TimeSeriesFor(deviceId, Device.Ts2);
                ts2.Append(baseline, new[] { 2d });

                session.Advanced.WaitForIndexesAfterSaveChanges();
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var devices = await session.Advanced.AsyncDocumentQuery<DeviceInfoIndexMapReduce.Result, DeviceInfoIndexMapReduce>().ToArrayAsync();
                Assert.Equal(1, devices[0].Ts1[0]);
                Assert.Equal(2, devices[0].Ts2[0]);
            }
        }

        [Fact]
        public async Task TimeSeriesMultiMapShouldWork()
        {
            var deviceId = "device/1";

            using var store = GetDocumentStore();
            await new DeviceInfoIndexMap().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Device { Id = deviceId });
                var baseline = DateTime.Now;
                var ts1 = session.TimeSeriesFor(deviceId, Device.Ts1);
                ts1.Append(baseline, new[] { 1d });

                var ts2 = session.TimeSeriesFor(deviceId, Device.Ts2);
                ts2.Append(baseline, new[] { 2d });

                session.Advanced.WaitForIndexesAfterSaveChanges();
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var devices = await session.Advanced.AsyncDocumentQuery<DeviceInfoIndexMap.Result, DeviceInfoIndexMap>().ToArrayAsync();
                Assert.Equal(1, devices[0].Ts1[0]);
                Assert.Equal(0, devices[0].Ts2.Length);

                Assert.Equal(2, devices[1].Ts2[0]);
                Assert.Equal(0, devices[1].Ts1.Length);
            }
        }


        public class Device
        {
            public const string Ts1 = "ts1";
            public const string Ts2 = "ts2";

            public string Id { get; set; }

        }

        public class DeviceInfoIndexMapReduce : Raven.Client.Documents.Indexes.TimeSeries.AbstractMultiMapTimeSeriesIndexCreationTask<DeviceInfoIndexMapReduce.Result>
        {
            public class Result
            {
                public string DeviceId { get; set; }
                public DateTime Timestamp { get; set; }
                public double[] Ts1 { get; set; }
                public double[] Ts2 { get; set; }
            }

            public DeviceInfoIndexMapReduce()
            {

                AddMap<Device>(Device.Ts1, value => from d in value
                    let last = d.Entries.Last()
                    select new Result {DeviceId = d.DocumentId, Timestamp = last.Timestamp, Ts1 = last.Values, Ts2 = new double[0]});

                AddMap<Device>(Device.Ts2, value => from d in value
                    let last = d.Entries.Last()
                    select new Result {DeviceId = d.DocumentId, Timestamp = last.Timestamp, Ts1 = new double[0], Ts2 = last.Values});

                Reduce = results => from result in results
                    group result by result.DeviceId
                    into g
                    let last = g.Last()
                    select new Result
                    {
                        DeviceId = g.Key,
                        Timestamp = last.Timestamp,
                        Ts1 = g.Where(r => r.Ts1.Length > 0).Select(r => r.Ts1).Last(),
                        Ts2 = g.Where(r => r.Ts2.Length > 0).Select(r => r.Ts2).Last(),
                    };
            }
        }

        public class DeviceInfoIndexMap : Raven.Client.Documents.Indexes.TimeSeries.AbstractMultiMapTimeSeriesIndexCreationTask<DeviceInfoIndexMap.Result>
        {
            public class Result
            {
                public string DeviceId { get; set; }
                public DateTime Timestamp { get; set; }
                public double[] Ts1 { get; set; }
                public double[] Ts2 { get; set; }
            }

            public DeviceInfoIndexMap()
            {

                AddMap<Device>(Device.Ts1, value => from d in value
                    let last = d.Entries.Last()
                    select new Result {DeviceId = d.DocumentId, Timestamp = last.Timestamp, Ts1 = last.Values, Ts2 = new double[0]});

                AddMap<Device>(Device.Ts2, value => from d in value
                    let last = d.Entries.Last()
                    select new Result {DeviceId = d.DocumentId, Timestamp = last.Timestamp, Ts1 = new double[0], Ts2 = last.Values});
            }
        }
    }
}
