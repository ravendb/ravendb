using System;
using System.Collections.Generic;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17418 : RavenTestBase
    {
        private class MyTimeSeriesResult
        {
            public int Count { get; set; }
            public Dictionary<string, double> Results { get; set; }
        }

        private class Label
        {
            public string Name;
            public string LabelId;
        }
        
        private class Subject
        {
            public Label[] Labels;
        }

        
        public RavenDB_17418(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanStreamTimeSeriesProjection()
        {
            RunTest(@"
                declare timeseries welliba(o)
                {
                    from o.Welliba between $start and $end
                    group by '1 days'
                    select avg()
                }

                declare function strip(ts)
                {
                    var n  = {};
                    var len = ts.Results.length;
                    for(var i = 0 ; i < len; i++){
                        n[ts.Results[i].From] = ts.Results[i].Average[0];
                    }
                    ts.Results = n;
                    return ts;
                }
                from Subjects as s
                where s.Labels[].LabelId all in ($l1, $l2)
                select strip(welliba(s))
            ");
        }

        [Fact]
        public void CanStreamTimeSeriesProjectionAndUseCountPropertyInProjection()
        {
            RunTest(@"
                declare timeseries welliba(o)
                {
                    from o.Welliba between $start and $end
                    group by '1 days'
                    select avg()
                }

                declare function strip(ts)
                {
                    var n  = {};
                    var len = ts.Results.Count;
                    for(var i = 0 ; i < len; i++){
                        n[ts.Results[i].From] = ts.Results[i].Average[0];
                    }
                    ts.Results = n;
                    return ts;
                }
                from Subjects as s
                where s.Labels[].LabelId all in ($l1, $l2)
                select strip(welliba(s))"
            );
        }

        private void RunTest(string rawQuery)
        {
            using var store = GetDocumentStore();
            var startDate = DateTime.Today;

            using (var s = store.OpenSession())
            {
                Subject subject = new Subject
                {
                    Labels = new[]
                    {
                        new Label{Name = "Active", LabelId = "labels/1"},
                        new Label{Name = "Running", LabelId = "labels/2"}

                    }
                };
                s.Store(subject);
                for (int i = 0; i < 100; i++)
                {
                    s.TimeSeriesFor(subject, "Welliba").Append(startDate.AddHours(i), i);
                }
                s.SaveChanges();
            }

            using (var session = store.OpenSession())
            {

                var endDate = startDate.AddMonths(1);

                var query = session.Advanced.RawQuery<MyTimeSeriesResult>(rawQuery)
                    .AddParameter("start", startDate)
                    .AddParameter("end", endDate)
                    .AddParameter("l1", "labels/1")
                    .AddParameter("l2", "labels/2");
                // this works
                List<MyTimeSeriesResult> timeSeriesResults = query.ToList();
                Assert.NotEmpty(timeSeriesResults);
                MyTimeSeriesResult firstResult = timeSeriesResults[0];

                WaitForUserToContinueTheTest(store);

                // this not
                bool hasResults = false;
                using (var docStream = session.Advanced.Stream(query))
                {
                    // throws exception here while calling MoveNext()
                    while (docStream.MoveNext())
                    {
                        var document = docStream.Current;
                        hasResults = true;
                    }
                }
                Assert.True(hasResults);
            }
        }
    }
}
