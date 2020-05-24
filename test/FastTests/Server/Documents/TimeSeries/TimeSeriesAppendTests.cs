using System;
using System.Collections.Generic;
using System.Text;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json.Parsing;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.TimeSeries
{
    public class TimeSeriesAppendTests : RavenLowLevelTestBase
    {
        public TimeSeriesAppendTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanAppendMoreThan127Tags()
        {
            using (var db = CreateDocumentDatabase())
            using (db.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext ctx))
            {
                var toAppend = new List<TimeSeriesStorage.Reader.SingleResult>();
                var baseline = DateTime.UtcNow;
                for (int i = 0; i < 256; i++)
                {
                    toAppend.Add(new TimeSeriesStorage.Reader.SingleResult
                    {
                        Timestamp = baseline.AddMinutes(i),
                        Tag = ctx.GetLazyString(i.ToString()),
                        Values = new double[]{1}
                    });
                }
                using (var tx = ctx.OpenWriteTransaction())
                {
                    var doc = ctx.ReadObject(new DynamicJsonValue(), "users/1");
                    db.DocumentsStorage.Put(ctx, "users/1", null, doc);
                    db.DocumentsStorage.TimeSeriesStorage.AppendTimestamp(ctx, "users/1", "@empty", "tags", toAppend);
                    tx.Commit();
                }

                using (var tx = ctx.OpenReadTransaction())
                {
                    var reader = db.DocumentsStorage.TimeSeriesStorage.GetReader(ctx, "users/1", "@empty", DateTime.MinValue, DateTime.MaxValue);
                    var i = 0;
                    foreach (var singleResult in reader.AllValues())
                    {
                        Assert.Equal(toAppend[i].Timestamp, singleResult.Timestamp);
                        Assert.Equal(toAppend[i].Values.Span[0], singleResult.Values.Span[0]);
                        Assert.Equal(toAppend[i].Tag.ToString(), singleResult.Tag.ToString());
                        i++;
                    }

                    var segments = db.DocumentsStorage.TimeSeriesStorage.GetNumberOfTimeSeriesSegments(ctx);
                    Assert.Equal(3, segments);
                }
            }
        }
    }
}
