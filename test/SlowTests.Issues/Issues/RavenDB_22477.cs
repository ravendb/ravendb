using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22477 : RavenTestBase
{
    public RavenDB_22477(ITestOutputHelper output) : base(output)
    {
    }

    private class MyCounterIndex : AbstractJavaScriptCountersIndexCreationTask
    {
        public MyCounterIndex()
        {
            Maps = new HashSet<string>
            {
                @"


counters.map('Companies', 'HeartRate', function (counter) {
return {
    HeartBeat: counter.Value,
    Name: counter.Name,
    User: counter.DocumentId
};
})"
            };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class AverageHeartRate : AbstractCountersIndexCreationTask<User, AverageHeartRate.Result>
    {
        public class Result
        {
            public double HeartBeat { get; set; }

            public string Name { get; set; }

            public long Count { get; set; }
        }

        public AverageHeartRate()
        {
            AddMap("HeartRate", counters => from counter in counters
                                            select new Result
                                            {
                                                HeartBeat = counter.Value,
                                                Count = 1,
                                                Name = counter.Name
                                            });

            Reduce = results => from r in results
                                group r by r.Name into g
                                let sumHeartBeat = g.Sum(x => x.HeartBeat)
                                let sumCount = g.Sum(x => x.Count)
                                select new Result
                                {
                                    HeartBeat = sumHeartBeat / sumCount,
                                    Name = g.Key,
                                    Count = sumCount
                                };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class CounterMapReduceIndexForMultipleCounterGroups : AbstractMultiMapCountersIndexCreationTask<CounterMapReduceIndexForMultipleCounterGroups.Result>
    {
        private const int N = 100;

        public class Result
        {
            public double Value { get; set; }
            public int Count { get; set; }
        }

        public CounterMapReduceIndexForMultipleCounterGroups()
        {
            for (var i = 0; i < N; i++)
            {
                AddMap<User>(
                    $"Counter{i}",
                    counters =>
                        from counter in counters
                        select new { Value = counter.Value, Count = 1 });
            }

            Reduce = results =>
                from result in results
                group result by new { result.Value }
                into g
                select new
                {
                    Value = g.Key.Value,
                    Count = g.Sum(r => r.Count)
                };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class MyTsIndex : AbstractJavaScriptTimeSeriesIndexCreationTask
    {
        public MyTsIndex()
        {
            Maps = new HashSet<string>
            {
                @"

timeSeries.map('Companies', 'HeartRate', function (ts) {
return ts.Entries.map(entry => ({
        HeartBeat: entry.Values[0],
        Date: new Date(entry.Timestamp.getFullYear(), entry.Timestamp.getMonth(), entry.Timestamp.getDate()),
        User: ts.DocumentId
    }));
})"
            };

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class MyTsIndex_Strong : AbstractTimeSeriesIndexCreationTask<Company>
    {
        public MyTsIndex_Strong()
        {
            AddMap(
                "HeartRate",
                timeSeries => from ts in timeSeries
                              from entry in ts.Entries
                              select new
                              {
                                  HeartBeat = entry.Values[0],
                                  entry.Timestamp.Date,
                                  User = ts.DocumentId
                              });

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    private class TimeSeriesIndex : AbstractMultiMapTimeSeriesIndexCreationTask<TimeSeriesIndex.Result>
    {
        public class Result
        {
            public string Name { get; set; }

            public double Value { get; set; }

            public DateTime Start { get; set; }

            public DateTime End { get; set; }
        }

        public TimeSeriesIndex()
        {
            AddMap<User>("Heartrate", segments => from ts in segments
                                                  from entry in ts.Entries
                                                  select new Result
                                                  {
                                                      Name = ts.Name,
                                                      Value = entry.Value,
                                                      Start = ts.Start,
                                                      End = ts.End
                                                  });

            AddMap<User>("Heartrate@By6Hours", segments => from ts in segments
                                                           from entry in ts.Entries
                                                           select new Result
                                                           {
                                                               Name = ts.Name,
                                                               Value = entry.Value,
                                                               Start = ts.Start,
                                                               End = ts.End
                                                           });

            AddMap<User>("Heartrate@By1Day", segments => from ts in segments
                                                         from entry in ts.Entries
                                                         select new Result
                                                         {
                                                             Name = ts.Name,
                                                             Value = entry.Value,
                                                             Start = ts.Start,
                                                             End = ts.End
                                                         });

            AddMap<User>("Heartrate@By30Minutes", segments => from ts in segments
                                                              from entry in ts.Entries
                                                              select new Result
                                                              {
                                                                  Name = ts.Name,
                                                                  Value = entry.Value,
                                                                  Start = ts.Start,
                                                                  End = ts.End
                                                              });

            AddMap<User>("Heartrate@By1Hour", segments => from ts in segments
                                                          from entry in ts.Entries
                                                          select new Result
                                                          {
                                                              Name = ts.Name,
                                                              Value = entry.Value,
                                                              Start = ts.Start,
                                                              End = ts.End
                                                          });

            SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType.Corax;
        }
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Indexes | RavenTestCategory.Counters)]
    public void Indexes_SearchEngineType_Is_Respected_Counters()
    {
        var index1 = new MyCounterIndex();
        var indexDefinition1 = index1.CreateIndexDefinition();

        Assert.Equal(SearchEngineType.Corax.ToString(), indexDefinition1.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)]);

        var index2 = new AverageHeartRate();
        var indexDefinition2 = index2.CreateIndexDefinition();

        Assert.Equal(SearchEngineType.Corax.ToString(), indexDefinition2.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)]);

        var index3 = new CounterMapReduceIndexForMultipleCounterGroups();
        var indexDefinition3 = index3.CreateIndexDefinition();

        Assert.Equal(SearchEngineType.Corax.ToString(), indexDefinition3.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)]);
    }

    [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
    public void Indexes_SearchEngineType_Is_Respected_TimeSeries()
    {
        var index1 = new MyTsIndex();
        var indexDefinition1 = index1.CreateIndexDefinition();

        Assert.Equal(SearchEngineType.Corax.ToString(), indexDefinition1.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)]);

        var index2 = new MyTsIndex_Strong();
        var indexDefinition2 = index2.CreateIndexDefinition();

        Assert.Equal(SearchEngineType.Corax.ToString(), indexDefinition2.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)]);

        var index3 = new TimeSeriesIndex();
        var indexDefinition3 = index3.CreateIndexDefinition();

        Assert.Equal(SearchEngineType.Corax.ToString(), indexDefinition3.Configuration[RavenConfiguration.GetKey(x => x.Indexing.StaticIndexingEngineType)]);
    }
}
