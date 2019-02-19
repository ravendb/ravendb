using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11501 : RavenTestBase
    {
        private class ActivitiesStateDetailByHourIndex : AbstractIndexCreationTask<ActivityEntryCollection, ActivityEntryDetail>
        {
            public ActivitiesStateDetailByHourIndex()
            {
                Map = activities => from activity in activities
                                    let hourlyGrouping = from state in activity.States.Where(x => x.Type != ActivityType.Idle)
                                                         let date = new DateTime(state.StartTicks)
                                                         let dateTicks = new DateTime(date.Year, date.Month, date.Day, date.Hour, 0, 0).Ticks
                                                         select new { Hour = dateTicks, State = state }

                                    let stateGrouping = from x in hourlyGrouping
                                                        group x by new { Hour = x.Hour, Type = x.State.Type } into g
                                                        select new
                                                        {
                                                            Type = g.Key.Type,
                                                            StartTicks = g.Key.Hour,
                                                            Collection = g.Select(x => new StateDurationDetail { Id = x.State.Id, StartTicks = x.State.StartTicks, Duration = x.State.DurationTicks })
                                                        }
                                    select new
                                    {
                                        Owner = activity.Owner,
                                        StartTick = stateGrouping.Min(x => x.StartTicks),
                                        Building = stateGrouping.Where(x => x.Type == ActivityType.Building).SelectMany(x => x.Collection),
                                        Debugging = stateGrouping.Where(x => x.Type == ActivityType.Debugging).SelectMany(x => x.Collection),
                                        Coding = stateGrouping.Where(x => x.Type == ActivityType.Coding).SelectMany(x => x.Collection),
                                        System = stateGrouping.Where(x => x.Type == ActivityType.System).SelectMany(x => x.Collection),
                                    };

                Reduce = results => from result in results
                                    group result by new { Owner = result.Owner, Hour = result.StartTick } into g
                                    select new ActivityEntryDetail
                                    {
                                        Owner = g.Key.Owner,
                                        StartTick = g.Key.Hour,
                                        Building = g.SelectMany(x => x.Building),
                                        Debugging = g.SelectMany(x => x.Debugging),
                                        Coding = g.SelectMany(x => x.Coding),
                                        System = g.SelectMany(x => x.System),
                                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class ActivityEntryCollection
        {
            public List<State> States { get; set; }
            public string Owner { get; set; }
        }

        private class ActivityEntryDetail
        {
            public string Owner { get; set; }
            public long StartTick { get; set; }
            public IEnumerable<StateDurationDetail> Building { get; set; }
            public IEnumerable<StateDurationDetail> Debugging { get; set; }
            public IEnumerable<StateDurationDetail> Coding { get; set; }
            public IEnumerable<StateDurationDetail> System { get; set; }
        }

        private class State
        {
            public string Id { get; set; }
            public ActivityType Type { get; set; }
            public long StartTicks { get; set; }
            public long DurationTicks { get; set; }
        }

        private enum ActivityType
        {
            Idle,
            Building,
            Debugging,
            Coding,
            System
        }

        private class StateDurationDetail
        {
            public string Id { get; set; }
            public long StartTicks { get; set; }
            public long Duration { get; set; }
        }

        [Fact]
        public void ShouldCompile()
        {
            using (var store = GetDocumentStore())
            {
                new ActivitiesStateDetailByHourIndex().Execute(store);
            }
        }
    }
}
