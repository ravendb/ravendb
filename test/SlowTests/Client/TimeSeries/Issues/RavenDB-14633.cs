﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using FastTests;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Http;
using Raven.Client.Json.Converters;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Extensions;
using Sparrow.Json;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.TimeSeries.Issues
{
    public class RavenDB_14633 : RavenTestBase
    {
        public RavenDB_14633(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanGetAll()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                    Appends = new List<TimeSeriesOperation.AppendOperation>()
                };

                for (int i = 0; i <= 360; i++)
                {
                    timeSeriesOp.Appends.Add(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddSeconds(i * 10),
                        Values = new[] { 59d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var re = store.GetRequestExecutor();
                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var tsCommand = new GetTimeSeriesCommand(documentId, new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "Heartrate",
                            From = DateTime.MinValue,
                            To = DateTime.MaxValue
                        }
                    }, 0, int.MaxValue);
                    re.Execute(tsCommand, context);
                    var res = tsCommand.Result;

                    Assert.Equal(361, res.Values["Heartrate"][0].TotalResults);
                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(1, res.Values["Heartrate"].Count);
                    Assert.Equal(361, res.Values["Heartrate"][0].Entries.Length);
                }
            }
        }

        [Fact]
        public void CanGetAll_WithPaging()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                    Appends = new List<TimeSeriesOperation.AppendOperation>()
                };

                for (int i = 0; i <= 360; i++)
                {
                    timeSeriesOp.Appends.Add(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddSeconds(i * 10),
                        Values = new[] { 59d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var re = store.GetRequestExecutor();
                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var tsCommand = new GetTimeSeriesCommand(documentId, new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "Heartrate",
                            From = DateTime.MinValue,
                            To = DateTime.MaxValue
                        }
                    }, start: 100, pageSize: 200);
                    re.Execute(tsCommand, context);
                    var res = tsCommand.Result;

                    Assert.Equal(361, res.Values["Heartrate"][0].TotalResults);
                    Assert.Equal(1, res.Values.Count);
                    Assert.Equal(1, res.Values["Heartrate"].Count);
                    Assert.Equal(200, res.Values["Heartrate"][0].Entries.Length);

                    Assert.Equal(baseline.AddSeconds(100 * 10), res.Values["Heartrate"][0].Entries[0].Timestamp);
                    Assert.Equal(baseline.AddSeconds(299 * 10), res.Values["Heartrate"][0].Entries[199].Timestamp);

                }
            }
        }

        [Fact]
        public void GetAllShouldReturnNotModifiedCode()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                    Appends = new List<TimeSeriesOperation.AppendOperation>()
                };

                for (int i = 0; i <= 360; i++)
                {
                    timeSeriesOp.Appends.Add(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddSeconds(i * 10),
                        Values = new[] { 59d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var re = store.GetRequestExecutor();
                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    for (int i = 0; i < 3; i++)
                    {
                        var tsCommand = new GetTimeSeriesCommand(documentId, new List<TimeSeriesRange>
                        {
                            new TimeSeriesRange
                            {
                                Name = "Heartrate",
                                From = DateTime.MinValue,
                                To = DateTime.MaxValue
                            }
                        }, start: 0, pageSize: int.MaxValue);

                        re.Execute(tsCommand, context);
                        var res = tsCommand.Result;

                        Assert.Equal(361, res.Values["Heartrate"][0].TotalResults);
                        Assert.Equal(1, res.Values.Count);
                        Assert.Equal(1, res.Values["Heartrate"].Count);
                        Assert.Equal(361, res.Values["Heartrate"][0].Entries.Length);

                        var statusCode = tsCommand.StatusCode;

                        if (i == 0)
                        {
                            Assert.Equal(HttpStatusCode.OK, statusCode);
                        }
                        else
                        {
                            Assert.Equal(HttpStatusCode.NotModified, statusCode);
                        }
                    }

                    using (var session = store.OpenSession())
                    {
                        // add a new entry to the series
                        session.TimeSeriesFor(documentId, "Heartrate").Append(baseline.AddSeconds(100).AddMilliseconds(50), new[] { 1000d }, "watches/apple");
                        session.SaveChanges();
                    }

                    // verify that we don't get cached results

                    var command = new GetTimeSeriesCommand(documentId, new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "Heartrate",
                            From = DateTime.MinValue,
                            To = DateTime.MaxValue
                        }
                    });
                    re.Execute(command, context);

                    Assert.Equal(HttpStatusCode.OK, command.StatusCode);

                    Assert.Equal(362, command.Result.Values["Heartrate"][0].TotalResults);
                    Assert.Equal(1, command.Result.Values.Count);
                    Assert.Equal(1, command.Result.Values["Heartrate"].Count);
                    Assert.Equal(362, command.Result.Values["Heartrate"][0].Entries.Length);

                    var newEntry = command.Result.Values["Heartrate"][0].Entries
                        .FirstOrDefault(e => e.Value == 1000d && e.Timestamp == baseline.AddSeconds(100).AddMilliseconds(50));

                    Assert.NotNull(newEntry);
                }


            }
        }

        [Fact]
        public void GetAllShouldReturnNotModifiedCode_WithPaging()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                    Appends = new List<TimeSeriesOperation.AppendOperation>()
                };

                for (int i = 0; i <= 360; i++)
                {
                    timeSeriesOp.Appends.Add(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddSeconds(i * 10),
                        Values = new[] { 59d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var re = store.GetRequestExecutor();
                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    GetTimeSeriesCommand tsCommand = default;
                    for (int i = 0; i < 3; i++)
                    {
                        tsCommand = new GetTimeSeriesCommand(documentId, new List<TimeSeriesRange>
                        {
                            new TimeSeriesRange
                            {
                                Name = "Heartrate",
                                From = DateTime.MinValue,
                                To = DateTime.MaxValue
                            }
                        }, start: 100, pageSize: 200);

                        re.Execute(tsCommand, context);
                        var res = tsCommand.Result;

                        Assert.Equal(361, res.Values["Heartrate"][0].TotalResults);
                        Assert.Equal(1, res.Values.Count);
                        Assert.Equal(1, res.Values["Heartrate"].Count);
                        Assert.Equal(200, res.Values["Heartrate"][0].Entries.Length);
                        Assert.Equal(baseline.AddSeconds(100 * 10), res.Values["Heartrate"][0].Entries[0].Timestamp);

                        var statusCode = tsCommand.StatusCode;

                        if (i == 0)
                        {
                            Assert.Equal(HttpStatusCode.OK, statusCode);
                        }
                        else
                        {
                            Assert.Equal(HttpStatusCode.NotModified, statusCode);
                        }
                    }

                    using (var session = store.OpenSession())
                    {
                        // add a new entry to the series
                        session.TimeSeriesFor(documentId, "Heartrate").Append(baseline.AddSeconds(2000).AddMilliseconds(50), new[] { 1000d }, "watches/apple");
                        session.SaveChanges();
                    }

                    // verify that we don't get cached results

                    tsCommand = new GetTimeSeriesCommand(documentId, new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "Heartrate",
                            From = DateTime.MinValue,
                            To = DateTime.MaxValue
                        }
                    }, start: 100, pageSize: 200);
                    re.Execute(tsCommand, context);

                    Assert.Equal(HttpStatusCode.OK, tsCommand.StatusCode);
                    Assert.Equal(362, tsCommand.Result.Values["Heartrate"][0].TotalResults);

                    var values = tsCommand.Result.Values;
                    Assert.Equal(1, values.Count);
                    Assert.Equal(1, values["Heartrate"].Count);
                    Assert.Equal(200, values["Heartrate"][0].Entries.Length);
                    Assert.Equal(baseline.AddSeconds(100 * 10), values["Heartrate"][0].Entries[0].Timestamp);

                    var newEntry = values["Heartrate"][0].Entries
                        .FirstOrDefault(e => e.Value == 1000d && e.Timestamp == baseline.AddSeconds(2000).AddMilliseconds(50));

                    Assert.NotNull(newEntry);

                    // request with a different 'start'
                    // verify that we don't get cached results

                    tsCommand = new GetTimeSeriesCommand(documentId, new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "Heartrate",
                            From = DateTime.MinValue,
                            To = DateTime.MaxValue
                        }
                    }, start: 101, pageSize: 200);
                    re.Execute(tsCommand, context);

                    Assert.Equal(HttpStatusCode.OK, tsCommand.StatusCode);
                    Assert.Equal(362, tsCommand.Result.Values["Heartrate"][0].TotalResults);

                    values = tsCommand.Result.Values;
                    Assert.Equal(1, values.Count);
                    Assert.Equal(1, values["Heartrate"].Count);
                    Assert.Equal(200, values["Heartrate"][0].Entries.Length);
                    Assert.Equal(baseline.AddSeconds(101 * 10), values["Heartrate"][0].Entries[0].Timestamp);
                }
            }
        }

        [Fact]
        public void CanGetRanges()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                    Appends = new List<TimeSeriesOperation.AppendOperation>()
                };

                for (int i = 0; i <= 360; i++)
                {
                    timeSeriesOp.Appends.Add(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddSeconds(i * 10),
                        Values = new[] { 59d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var re = store.GetRequestExecutor();
                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var ranges = new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(5), To = baseline.AddMinutes(10)
                        },
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(15), To = baseline.AddMinutes(30)
                        },
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(40), To = baseline.AddMinutes(60)
                        }
                    };

                    var tsCommand = new GetTimeSeriesCommand(documentId, ranges, 0, int.MaxValue);
                    re.Execute(tsCommand, context);
                    var timesSeriesDetails = tsCommand.Result;

                    Assert.Equal(documentId, timesSeriesDetails.Id);
                    Assert.Equal(1, timesSeriesDetails.Values.Count);
                    Assert.Equal(3, timesSeriesDetails.Values["Heartrate"].Count);

                    Assert.Null(timesSeriesDetails.Values["Heartrate"][0].TotalResults);

                    var range = timesSeriesDetails.Values["Heartrate"][0];

                    Assert.Equal(baseline.AddMinutes(5), range.From);
                    Assert.Equal(baseline.AddMinutes(10), range.To);

                    Assert.Equal(31, range.Entries.Length);
                    Assert.Equal(baseline.AddMinutes(5), range.Entries[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(10), range.Entries[30].Timestamp);

                    range = timesSeriesDetails.Values["Heartrate"][1];

                    Assert.Equal(baseline.AddMinutes(15), range.From);
                    Assert.Equal(baseline.AddMinutes(30), range.To);

                    Assert.Equal(91, range.Entries.Length);
                    Assert.Equal(baseline.AddMinutes(15), range.Entries[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(30), range.Entries[90].Timestamp);

                    range = timesSeriesDetails.Values["Heartrate"][2];

                    Assert.Equal(baseline.AddMinutes(40), range.From);
                    Assert.Equal(baseline.AddMinutes(60), range.To);

                    Assert.Equal(121, range.Entries.Length);
                    Assert.Equal(baseline.AddMinutes(40), range.Entries[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(60), range.Entries[120].Timestamp);
                }
            }
        }

        [Fact]
        public void CanGetRanges_WithPaging()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                    Appends = new List<TimeSeriesOperation.AppendOperation>()
                };

                for (int i = 0; i <= 360; i++)
                {
                    timeSeriesOp.Appends.Add(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddSeconds(i * 10),
                        Values = new[] { 59d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var re = store.GetRequestExecutor();
                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var ranges = new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(5), To = baseline.AddMinutes(10)
                        },
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(15), To = baseline.AddMinutes(30)
                        },
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(40), To = baseline.AddMinutes(60)
                        }
                    };

                    var tsCommand = new GetTimeSeriesCommand(documentId, ranges, 10, 150);
                    re.Execute(tsCommand, context);
                    var timesSeriesDetails = tsCommand.Result;

                    Assert.Equal(documentId, timesSeriesDetails.Id);
                    Assert.Equal(1, timesSeriesDetails.Values.Count);
                    Assert.Equal(3, timesSeriesDetails.Values["Heartrate"].Count);

                    Assert.Null(timesSeriesDetails.Values["Heartrate"][0].TotalResults);

                    var range = timesSeriesDetails.Values["Heartrate"][0];

                    Assert.Equal(baseline.AddMinutes(5), range.From);
                    Assert.Equal(baseline.AddMinutes(10), range.To);

                    Assert.Equal(21, range.Entries.Length);
                    Assert.Equal(baseline.AddMinutes(6).AddSeconds(40), range.Entries[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(10), range.Entries[20].Timestamp);

                    range = timesSeriesDetails.Values["Heartrate"][1];

                    Assert.Equal(baseline.AddMinutes(15), range.From);
                    Assert.Equal(baseline.AddMinutes(30), range.To);

                    Assert.Equal(91, range.Entries.Length);
                    Assert.Equal(baseline.AddMinutes(15), range.Entries[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(30), range.Entries[90].Timestamp);

                    range = timesSeriesDetails.Values["Heartrate"][2];

                    Assert.Equal(baseline.AddMinutes(40), range.From);
                    Assert.Equal(baseline.AddMinutes(60), range.To);

                    Assert.Equal(38, range.Entries.Length);
                    Assert.Equal(baseline.AddMinutes(40), range.Entries[0].Timestamp);
                    Assert.Equal(baseline.AddMinutes(46).AddSeconds(10), range.Entries[37].Timestamp);
                }
            }
        }

        [Fact]
        public void GetRangesShouldReturnNotModifiedCode()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                    Appends = new List<TimeSeriesOperation.AppendOperation>()
                };

                for (int i = 0; i <= 360; i++)
                {
                    timeSeriesOp.Appends.Add(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddSeconds(i * 10),
                        Values = new[] { 59d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var re = store.GetRequestExecutor();
                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var ranges = new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(5), To = baseline.AddMinutes(10)
                        },
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(15), To = baseline.AddMinutes(30)
                        },
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(40), To = baseline.AddMinutes(60)
                        }
                    };

                    GetTimeSeriesCommand tsCommand;

                    for (int i = 0; i < 3; i++)
                    {
                        tsCommand = new GetTimeSeriesCommand(documentId, ranges);
                        re.Execute(tsCommand, context);
                        var timesSeriesDetails = tsCommand.Result;

                        if (i == 0)
                        {
                            Assert.Equal(HttpStatusCode.OK, tsCommand.StatusCode);
                        }
                        else
                        {
                            Assert.Equal(HttpStatusCode.NotModified, tsCommand.StatusCode);
                        }

                        Assert.Equal(documentId, timesSeriesDetails.Id);
                        Assert.Equal(1, timesSeriesDetails.Values.Count);
                        Assert.Equal(3, timesSeriesDetails.Values["Heartrate"].Count);

                        var range = timesSeriesDetails.Values["Heartrate"][0];

                        Assert.Equal(baseline.AddMinutes(5), range.From);
                        Assert.Equal(baseline.AddMinutes(10), range.To);

                        Assert.Equal(31, range.Entries.Length);
                        Assert.Equal(baseline.AddMinutes(5), range.Entries[0].Timestamp);
                        Assert.Equal(baseline.AddMinutes(10), range.Entries[30].Timestamp);

                        range = timesSeriesDetails.Values["Heartrate"][1];

                        Assert.Equal(baseline.AddMinutes(15), range.From);
                        Assert.Equal(baseline.AddMinutes(30), range.To);

                        Assert.Equal(91, range.Entries.Length);
                        Assert.Equal(baseline.AddMinutes(15), range.Entries[0].Timestamp);
                        Assert.Equal(baseline.AddMinutes(30), range.Entries[90].Timestamp);

                        range = timesSeriesDetails.Values["Heartrate"][2];

                        Assert.Equal(baseline.AddMinutes(40), range.From);
                        Assert.Equal(baseline.AddMinutes(60), range.To);

                        Assert.Equal(121, range.Entries.Length);
                        Assert.Equal(baseline.AddMinutes(40), range.Entries[0].Timestamp);
                        Assert.Equal(baseline.AddMinutes(60), range.Entries[120].Timestamp);
                    }

                    using (var session = store.OpenSession())
                    {
                        // add a new entry to the series
                        session.TimeSeriesFor(documentId, "Heartrate").Append(baseline.AddMinutes(5).AddMilliseconds(50), new[] { 1000d }, "watches/apple");
                        session.SaveChanges();
                    }

                    // verify that we don't get cached results

                    tsCommand = new GetTimeSeriesCommand(documentId, ranges);
                    re.Execute(tsCommand, context);

                    Assert.Equal(HttpStatusCode.OK, tsCommand.StatusCode);


                    var newEntry = tsCommand.Result.Values["Heartrate"][0].Entries
                        .FirstOrDefault(e => e.Value == 1000d && e.Timestamp == baseline.AddMinutes(5).AddMilliseconds(50));

                    Assert.NotNull(newEntry);

                }
            }
        }

        [Fact]
        public void GetRangesShouldReturnNotModifiedCode_WithPaging()
        {
            const string documentId = "users/ayende";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);
                    session.SaveChanges();
                }

                var baseline = DateTime.Today;

                var timeSeriesOp = new TimeSeriesOperation
                {
                    Name = "Heartrate",
                    Appends = new List<TimeSeriesOperation.AppendOperation>()
                };

                for (int i = 0; i <= 360; i++)
                {
                    timeSeriesOp.Appends.Add(new TimeSeriesOperation.AppendOperation
                    {
                        Tag = "watches/fitbit",
                        Timestamp = baseline.AddSeconds(i * 10),
                        Values = new[] { 59d }
                    });
                }

                var timeSeriesBatch = new TimeSeriesBatchOperation(documentId, timeSeriesOp);

                store.Operations.Send(timeSeriesBatch);

                var re = store.GetRequestExecutor();
                using (re.ContextPool.AllocateOperationContext(out var context))
                {
                    var ranges = new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(5), To = baseline.AddMinutes(10)
                        },
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(15), To = baseline.AddMinutes(30)
                        },
                        new TimeSeriesRange
                        {
                            Name = "Heartrate", From = baseline.AddMinutes(40), To = baseline.AddMinutes(60)
                        }
                    };

                    GetTimeSeriesCommand tsCommand;

                    for (int i = 0; i < 3; i++)
                    {
                        tsCommand = new GetTimeSeriesCommand(documentId, ranges, 10, 150);
                        re.Execute(tsCommand, context);
                        var timesSeriesDetails = tsCommand.Result;

                        if (i == 0)
                        {
                            Assert.Equal(HttpStatusCode.OK, tsCommand.StatusCode);
                        }
                        else
                        {
                            Assert.Equal(HttpStatusCode.NotModified, tsCommand.StatusCode);
                        }

                        Assert.Equal(documentId, timesSeriesDetails.Id);
                        Assert.Equal(1, timesSeriesDetails.Values.Count);
                        Assert.Equal(3, timesSeriesDetails.Values["Heartrate"].Count);

                        var range = timesSeriesDetails.Values["Heartrate"][0];

                        Assert.Equal(baseline.AddMinutes(5), range.From);
                        Assert.Equal(baseline.AddMinutes(10), range.To);

                        Assert.Equal(21, range.Entries.Length);
                        Assert.Equal(baseline.AddMinutes(6).AddSeconds(40), range.Entries[0].Timestamp);
                        Assert.Equal(baseline.AddMinutes(10), range.Entries[20].Timestamp);

                        range = timesSeriesDetails.Values["Heartrate"][1];

                        Assert.Equal(baseline.AddMinutes(15), range.From);
                        Assert.Equal(baseline.AddMinutes(30), range.To);

                        Assert.Equal(91, range.Entries.Length);
                        Assert.Equal(baseline.AddMinutes(15), range.Entries[0].Timestamp);
                        Assert.Equal(baseline.AddMinutes(30), range.Entries[90].Timestamp);

                        range = timesSeriesDetails.Values["Heartrate"][2];

                        Assert.Equal(baseline.AddMinutes(40), range.From);
                        Assert.Equal(baseline.AddMinutes(60), range.To);

                        Assert.Equal(38, range.Entries.Length);
                        Assert.Equal(baseline.AddMinutes(40), range.Entries[0].Timestamp);
                        Assert.Equal(baseline.AddMinutes(46).AddSeconds(10), range.Entries[37].Timestamp);

                    }

                    using (var session = store.OpenSession())
                    {
                        // add a new entry to the series
                        session.TimeSeriesFor(documentId, "Heartrate").Append(baseline.AddMinutes(15).AddMilliseconds(50), new[] { 1000d }, "watches/apple");
                        session.SaveChanges();
                    }

                    // verify that we don't get cached results

                    tsCommand = new GetTimeSeriesCommand(documentId, ranges, 10, 150);
                    re.Execute(tsCommand, context);

                    Assert.Equal(HttpStatusCode.OK, tsCommand.StatusCode);

                    var newEntry = tsCommand.Result.Values["Heartrate"][1].Entries
                        .FirstOrDefault(e => e.Value == 1000d && e.Timestamp == baseline.AddMinutes(15).AddMilliseconds(50));

                    Assert.NotNull(newEntry);

                    // request with a different 'start'
                    // verify that we don't get cached results

                    tsCommand = new GetTimeSeriesCommand(documentId, new List<TimeSeriesRange>
                    {
                        new TimeSeriesRange()
                        {
                            Name = "Heartrate",
                            From = DateTime.MinValue,
                            To = DateTime.MaxValue
                        }
                    }, start: 12, pageSize: 150);
                    re.Execute(tsCommand, context);

                    Assert.Equal(HttpStatusCode.OK, tsCommand.StatusCode);

                    Assert.Equal(baseline.AddSeconds(12 * 10), tsCommand.Result.Values["Heartrate"][0].Entries[0].Timestamp);

                }
            }
        }

        private class GetTimeSeriesCommand : RavenCommand<TimeSeriesDetails>
        {
            private readonly string _docId;
            private readonly string _timeseries;
            private readonly IEnumerable<TimeSeriesRange> _ranges;
            private readonly int _start;
            private readonly int _pageSize;

            public GetTimeSeriesCommand(string docId, IEnumerable<TimeSeriesRange> ranges, int start = 0, int pageSize = int.MaxValue)
            {
                _docId = docId ?? throw new ArgumentNullException(nameof(docId));
                _ranges = ranges ?? throw new ArgumentNullException(nameof(ranges));
                _start = start;
                _pageSize = pageSize;
            }

            public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
            {
                var pathBuilder = new StringBuilder(node.Url);
                pathBuilder.Append("/databases/")
                    .Append(node.Database)
                    .Append("/timeseries")
                    .Append("?id=")
                    .Append(Uri.EscapeDataString(_docId));

                if (_start > 0)
                {
                    pathBuilder.Append("&start=")
                        .Append(_start);
                }

                if (_pageSize < int.MaxValue)
                {
                    pathBuilder.Append("&pageSize=")
                        .Append(_pageSize);
                }

                foreach (var range in _ranges)
                {
                    pathBuilder.Append("&name=")
                        .Append(range.Name)
                        .Append("&from=")
                        .Append(range.From.GetDefaultRavenFormat())
                        .Append("&to=")
                        .Append(range.To.GetDefaultRavenFormat());
                }

                var request = new HttpRequestMessage
                {
                    Method = HttpMethod.Get
                };

                url = pathBuilder.ToString();

                return request;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    return;

                Result = JsonDeserializationClient.TimeSeriesDetails(response);
            }

            public override bool IsReadRequest => true;
        }

    }
}
