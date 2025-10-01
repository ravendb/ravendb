using System;
using System.Collections.Generic;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Counters;
using Raven.Client.Documents.Indexes.TimeSeries;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;


namespace SlowTests.Issues
{
    public class RavenDB_20593 : RavenTestBase
    {

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
            }
        }

        private class MySimpleIndex : AbstractJavaScriptIndexCreationTask
        {
            public MySimpleIndex()
            {
                Maps = new HashSet<string>
                {
                    @"










map(""Companies"", (company) => {
        return {
            Name: company.Name,
            Phone: company.Phone,
            City: company.Address.City
        };  
})"
                };
            }
        }

        public RavenDB_20593(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Indexes)]
        public void JsIndexWithNewLinesAtTheBeginningShouldBeRespected()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var now = DateTime.Now;

                    var company1 = new Company();
                    session.Store(company1, "companies/1");
                    session.CountersFor(company1).Increment("HeartRate", 3);
                    session.TimeSeriesFor(company1, "HeartRate").Append(now, new double[] { 7 });

                    var company2 = new Company();
                    session.Store(company2, "companies/2");
                    session.CountersFor(company2).Increment("HeartRate", 4);
                    session.TimeSeriesFor(company2, "HeartRate").Append(now, new double[] { 7 });

                    session.SaveChanges();

                    var counterIndex = new MyCounterIndex();
                    var tsIndex = new MyTsIndex();
                    var simpleIndex = new MySimpleIndex();

                    counterIndex.Execute(store);
                    tsIndex.Execute(store);
                    simpleIndex.Execute(store);

                    Indexes.WaitForIndexing(store);

                    var stats1 = store.Maintenance.Send(new GetIndexStatisticsOperation("MyCounterIndex"));
                    Assert.Equal(2, stats1.EntriesCount);

                    var stats2 = store.Maintenance.Send(new GetIndexStatisticsOperation("MyTsIndex"));
                    Assert.Equal(2, stats2.EntriesCount);

                    var stats3 = store.Maintenance.Send(new GetIndexStatisticsOperation("MySimpleIndex"));
                    Assert.Equal(2, stats3.EntriesCount);
                }

            }
        }
    }
}
