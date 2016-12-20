// -----------------------------------------------------------------------
//  <copyright file="TimeSeriesOperations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
using Raven.Abstractions.TimeSeries;
using Raven.Database.TimeSeries;
using Raven.Tests.Common;
using Xunit;

namespace Raven.Tests.TimeSeries
{
    public class TimeSeriesOperations : RavenBaseTimeSeriesTest
    {
        [Fact]
        public async Task SimpleAppend()
        {
            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                await store.CreateTypeAsync("FourValues", new[] { "Value 1", "Value Two", "Value Three", "Value 4" });

                await store.AppendAsync("FourValues", "Time", DateTimeOffset.Now, new[] { 3D, 4D, 5D, 6D });
                await store.AppendAsync("Simple", "Is", DateTimeOffset.Now, 3D);
                await store.AppendAsync("Simple", "Money", DateTimeOffset.Now, 3D);
                
                var cancellationToken = new CancellationToken();
                await store.AppendAsync("Simple", "Is", DateTimeOffset.Now, 3456D, cancellationToken);
                await store.AppendAsync("FourValues", "Time", DateTimeOffset.Now, new[] { 23D, 4D, 5D, 6D }, cancellationToken);
                await store.AppendAsync("FourValues", "Time", DateTimeOffset.Now, cancellationToken, 33D, 4D, 5D, 6D);

                var stats = await store.GetStatsAsync(cancellationToken);
                Assert.Equal(2, stats.TypesCount);
                Assert.Equal(3, stats.KeysCount);
                Assert.Equal(6, stats.PointsCount);
            }
        }

        [Fact]
        public async Task ShouldNotAllowOverwriteType_OnlyIfTypeHasKeys()
        {
            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                await store.CreateTypeAsync("Simple", new[] {"Value", "Another value"}); // Can overwrite, because we don't have keys yet
                await store.AppendAsync("Simple", "Is", DateTimeOffset.Now, CancellationToken.None, 3456D, 8888D);
                var exception = await AssertAsync.Throws<ErrorResponseException>(async () => await store.CreateTypeAsync("Simple", new[] { "Value", "Another value", "Different Value" }));
                Assert.Contains("System.InvalidOperationException: There an existing type with the same name but with different fields. Since the type has already 1 keys, the replication failed to overwrite this type.", exception.Message);

                var stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(1, stats.KeysCount);
                Assert.Equal(1, stats.PointsCount);
            }
        }

        [Fact]
        public async Task SimpleAppend_ShouldFailIfTwoKeysAsDifferentValuesLength()
        {
            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                await store.AppendAsync("Simple", "Time", DateTimeOffset.Now, 3D);

                var exception = await AssertAsync.Throws<ErrorResponseException>(async () => await store.AppendAsync("Simple", "Time", DateTimeOffset.Now, new[] { 3D, 4D, 5D, 6D }));
                Assert.Contains("System.ArgumentOutOfRangeException: Appended values should be the same length the series values length which is 1 and not 4", exception.Message);

                var stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(1, stats.KeysCount);
                Assert.Equal(1, stats.PointsCount);
            }
        }

        [Fact]
        public async Task AddAndDeleteType_ShouldThrowIfTypeHasData()
        {
            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                await store.AppendAsync("Simple", "Is", DateTimeOffset.Now, 3D);

                var exception = await AssertAsync.Throws<ErrorResponseException>(async () => await store.DeleteTypeAsync("Simple"));
                Assert.Contains("System.InvalidOperationException: Cannot delete type 'Simple' because it has 1 associated key.", exception.Message);

                var stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(1, stats.KeysCount);
                Assert.Equal(1, stats.PointsCount);
            }
        }

        [Fact]
        public async Task AddAndDeleteType()
        {
            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                await store.AppendAsync("Simple", "Is", DateTimeOffset.Now, 3D);
                await store.DeleteKeyAsync("Simple", "Is");
                await store.DeleteTypeAsync("Simple");

                var stats = await store.GetStatsAsync();
                Assert.Equal(0, stats.TypesCount);
                Assert.Equal(0, stats.KeysCount);
                Assert.Equal(0, stats.PointsCount);
            }
        }

        [Fact]
        public async Task DeletePoints()
        {
            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                var start = DateTimeOffset.Now;
                for (int i = 0; i < 5; i++)
                {
                    await store.AppendAsync("Simple", "Is", start.AddMinutes(i), 3D);
                }
                var stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(1, stats.KeysCount);
                Assert.Equal(5, stats.PointsCount);

                await store.DeletePointAsync("Simple", "Is", start.AddMinutes(2));
                await store.DeletePointsAsync(new[]
                {
                    new TimeSeriesPointId
                    {
                        Type = "Simple", Key = "Is", At = start.AddMinutes(3)
                    },
                    new TimeSeriesPointId
                    {
                        Type = "Simple", Key = "Is", At = start.AddMinutes(4)
                    },
                });

                stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(1, stats.KeysCount);
                Assert.Equal(2, stats.PointsCount);
            }
        }

        [Fact]
        public async Task DeleteRange()
        {
            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                
                var start = new DateTimeOffset(2015, 1, 1, 0, 0, 0, TimeSpan.Zero);
                for (int i = 0; i < 12; i++)
                {
                    await store.AppendAsync("Simple", "Time", start.AddHours(i), i + 3D);
                }
                var stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(1, stats.KeysCount);
                Assert.Equal(12, stats.PointsCount);

                await store.DeleteRangeAsync("Simple", "Time", start.AddHours(3), start.AddHours(7));
                stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(1, stats.KeysCount);
                Assert.Equal(7, stats.PointsCount);
            }
        }

        [Fact]
        public async Task DeleteBigRange()
        {
            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                
                var start = new DateTimeOffset(2015, 1, 1, 0, 0, 0, TimeSpan.Zero);
                for (int i = 0; i < 1200; i++)
                {
                    await store.AppendAsync("Simple", "Time", start.AddHours(i), i + 3D);
                }
                var stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(1, stats.KeysCount);
                Assert.Equal(1200, stats.PointsCount);

                await store.DeleteRangeAsync("Simple", "Time", start.AddHours(3), start.AddYears(2));
                stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(3, stats.PointsCount);
                Assert.Equal(1, stats.KeysCount);
            }
        }

        [Fact]
        public async Task DeleteBigRange_DeleteAll()
        {
            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });

                var start = new DateTimeOffset(2015, 1, 1, 0, 0, 0, TimeSpan.Zero);
                for (int i = 0; i < 1200; i++)
                {
                    await store.AppendAsync("Simple", "Time", start.AddHours(i), i + 3D);
                }
                var stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(1, stats.KeysCount);
                Assert.Equal(1200, stats.PointsCount);

                await store.DeleteRangeAsync("Simple", "Time", DateTimeOffset.MinValue, DateTimeOffset.MaxValue);
                stats = await store.GetStatsAsync();
                Assert.Equal(1, stats.TypesCount);
                Assert.Equal(0, stats.KeysCount);
                Assert.Equal(0, stats.PointsCount);
            }
        }

        [Fact]
        public async Task AdvancedAppend()
        {
            var start = DateTimeOffset.Now;

            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                await store.CreateTypeAsync("FourValues", new[] { "Value 1", "Value Two", "Value Three", "Value 4" });

                using (var batch = store.Advanced.NewBatch(new TimeSeriesBatchOptions { }))
                {
                    for (int i = 0; i < 18888; i++)
                    {
                        batch.ScheduleAppend("Simple", "Is", start.AddSeconds(i + 1), 3D);
                        batch.ScheduleAppend("Simple", "Money", start.AddSeconds(i + 18), 13D);
                        batch.ScheduleAppend("FourValues", "Time", start.AddSeconds(i + 13), 3D, 4D, 5D, 6D);
                    }
                    await batch.FlushAsync();
                }

                var types = await store.Advanced.GetTypes();
                Assert.Equal(2, types.Length);

                var fourValues = types[0];
                Assert.Equal("FourValues", fourValues.Type);
                Assert.Equal(4, fourValues.Fields.Length);
                Assert.Equal(1, fourValues.KeysCount);
                var keys = await store.Advanced.GetKeys(fourValues.Type);
                Assert.Equal(1, keys.Length); var time = keys[0];
                Assert.Equal("FourValues", time.Type.Type);
                Assert.Equal("Time", time.Key);
                Assert.Equal(18888, time.PointsCount);

                var simple = types[1];
                Assert.Equal("Simple", simple.Type);
                Assert.Equal(1, simple.Fields.Length);
                Assert.Equal(2, simple.KeysCount);
                keys = await store.Advanced.GetKeys(simple.Type);
                Assert.Equal(2, keys.Length);
                var _is = keys[0];
                Assert.Equal("Simple", _is.Type.Type);
                Assert.Equal("Is", _is.Key);
                Assert.Equal(18888, _is.PointsCount);
                var money = keys[1];
                Assert.Equal("Simple", money.Type.Type);
                Assert.Equal("Money", money.Key);
                Assert.Equal(18888, money.PointsCount);

                var stats = await store.GetStatsAsync();
                Assert.Equal(2, stats.TypesCount);
                Assert.Equal(3, stats.KeysCount);
                Assert.Equal(18888 * 3, stats.PointsCount);

                WaitForUserToContinueTheTest(startPage: "/studio/index.html#timeseries/series?type=-Simple&key=Money&timeseries=SeriesName-1");
            }
        }

        [Fact]
        public async Task GetKeys()
        {
            var start = new DateTimeOffset(2015, 10, 7, 15, 0, 0, TimeSpan.Zero);

            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                await store.CreateTypeAsync("FourValues", new[] { "Value 1", "Value Two", "Value Three", "Value 4" });

                await store.AppendAsync("FourValues", "Time", start, new[] { 3D, 4D, 5D, 6D });
                await store.AppendAsync("Simple", "Is", start, 3D);
                await store.AppendAsync("Simple", "Money", start, 3D);

                var cancellationToken = new CancellationToken();
                await store.AppendAsync("Simple", "Is", start.AddHours(1), 3456D, cancellationToken);
                await store.AppendAsync("FourValues", "Time", start.AddHours(1), new[] { 23D, 4D, 5D, 6D }, cancellationToken);
                await store.AppendAsync("FourValues", "Time", start.AddHours(2), cancellationToken, 33D, 4D, 5D, 6D);
                await store.AppendAsync("FourValues", "Time", start.AddHours(3), cancellationToken, 33D, 4D, 5D, 6D);

                var types = await store.Advanced.GetTypes(cancellationToken);
                Assert.Equal(2, types.Length);

                var fourValues = types[0];
                Assert.Equal("FourValues", fourValues.Type);
                Assert.Equal(4, fourValues.Fields.Length);
                Assert.Equal(1, fourValues.KeysCount);
                var keys = await store.Advanced.GetKeys(fourValues.Type);
                Assert.Equal(1, keys.Length);
                var time = keys[0];
                Assert.Equal("FourValues", time.Type.Type);
                Assert.Equal("Time", time.Key);
                Assert.Equal(4, time.PointsCount);
                var points = await store.Advanced.GetPoints(fourValues.Type, time.Key);
                Assert.Equal(points.Length, time.PointsCount);
                var aggregatedPoints = await store.Advanced.GetAggregatedPoints(fourValues.Type, time.Key, AggregationDuration.Hours(2), start.AddHours(-1), start.AddHours(3));
                Assert.Equal(aggregatedPoints.Length, 2);
                Assert.Equal(aggregatedPoints[0].Values[0].Volume, 1);
                Assert.Equal(aggregatedPoints[1].Values[0].Volume, 2);

                var simple = types[1];
                Assert.Equal("Simple", simple.Type);
                Assert.Equal(1, simple.Fields.Length);
                Assert.Equal(2, simple.KeysCount);
                keys = await store.Advanced.GetKeys(simple.Type);
                Assert.Equal(2, keys.Length);
                var _is = keys[0];
                Assert.Equal("Simple", _is.Type.Type);
                Assert.Equal("Is", _is.Key);
                Assert.Equal(2, _is.PointsCount);
                var money = keys[1];
                Assert.Equal("Simple", money.Type.Type);
                Assert.Equal("Money", money.Key);
                Assert.Equal(1, money.PointsCount);
                points = await store.Advanced.GetPoints(fourValues.Type, money.Key, start: DateTimeOffset.MinValue, end: DateTimeOffset.MaxValue, skip: 1, take: int.MaxValue, token: cancellationToken);
                Assert.Equal(points.Length, money.PointsCount - 1);

                var stats = await store.GetStatsAsync(cancellationToken);
                Assert.Equal(2, stats.TypesCount);
                Assert.Equal(3, stats.KeysCount);
                Assert.Equal(7, stats.PointsCount);
            }
        }

        [Fact]
        public async Task CanOverrideExistingPoints()
        {
            var start = DateTimeOffset.Now;

            using (var store = NewRemoteTimeSeriesStore())
            {
                await store.CreateTypeAsync("Simple", new[] { "Value" });
                await store.CreateTypeAsync("FourValues", new[] { "Value 1", "Value Two", "Value Three", "Value 4" });

                await store.AppendAsync("FourValues", "Time", start, new[] { 3D, 4D, 5D, 6D });
                await store.AppendAsync("Simple", "Is", start, 3D);
                await store.AppendAsync("Simple", "Money", start, 3D);

                using (var batch = store.Advanced.NewBatch(new TimeSeriesBatchOptions { }))
                {
                    for (int i = 0; i < 1888; i++)
                    {
                        await store.AppendAsync("FourValues", "Time", start, new[] { 3D, 4D, 5D, 6D });
                        await store.AppendAsync("Simple", "Is", start, 3D);
                        await store.AppendAsync("Simple", "Money", start, 3D);

                        batch.ScheduleAppend("Simple", "Is", start, 3D);
                        batch.ScheduleAppend("Simple", "Money", start, 13D);
                        batch.ScheduleAppend("FourValues", "Time", start, 3D, 4D, 5D, 6D);
                    }
                    await batch.FlushAsync();
                }

                var cancellationToken = new CancellationToken();
                await store.AppendAsync("Simple", "Is", start.AddYears(10), 3456D, cancellationToken);
                await store.AppendAsync("FourValues", "Time", start.AddYears(1), new[] { 23D, 4D, 5D, 6D }, cancellationToken);
                await store.AppendAsync("FourValues", "Time", start.AddYears(2), cancellationToken, 33D, 4D, 5D, 6D);
                await store.AppendAsync("FourValues", "Time", start.AddYears(3), cancellationToken, 33D, 4D, 5D, 6D);

                var types = await store.Advanced.GetTypes(cancellationToken);
                Assert.Equal(2, types.Length);

                var fourValues = types[0];
                Assert.Equal("FourValues", fourValues.Type);
                Assert.Equal(4, fourValues.Fields.Length);
                Assert.Equal(1, fourValues.KeysCount);
                var keys = await store.Advanced.GetKeys(fourValues.Type);
                Assert.Equal(1, keys.Length); var time = keys[0];
                Assert.Equal("FourValues", time.Type.Type);
                Assert.Equal("Time", time.Key);
                Assert.Equal(4, time.PointsCount);

                var simple = types[1];
                Assert.Equal("Simple", simple.Type);
                Assert.Equal(1, simple.Fields.Length);
                Assert.Equal(2, simple.KeysCount);
                keys = await store.Advanced.GetKeys(simple.Type, cancellationToken);
                Assert.Equal(2, keys.Length);
                var _is = keys[0];
                Assert.Equal("Simple", _is.Type.Type);
                Assert.Equal("Is", _is.Key);
                Assert.Equal(2, _is.PointsCount);
                var money = keys[1];
                Assert.Equal("Simple", money.Type.Type);
                Assert.Equal("Money", money.Key);
                Assert.Equal(1, money.PointsCount);

                var stats = await store.GetStatsAsync(cancellationToken);
                Assert.Equal(2, stats.TypesCount);
                Assert.Equal(3, stats.KeysCount);
                Assert.Equal(7, stats.PointsCount);
            }
        }
    }
}
