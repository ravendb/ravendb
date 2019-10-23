using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_6095 : RavenTestBase
    {
        public RavenDB_6095(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldCompile()
        {
            using (var store = GetDocumentStore())
            {
                new ActivitiesStateDetailByHourIndex().Execute(store);
            }
        }

        public class ActivitiesStateDetailByHourIndex : AbstractIndexCreationTask
        {
            public override string IndexName => "ActivitiesStateDetailByHourIndex";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps = { @"from activity in docs.ActivityEntryCollections
select new {
    activity = activity,
    hourlyGrouping = activity.States.Where(x => x.Type != ""Idle"").Select(state => new {
        state = state,
        date = new DateTime(state.StartTicks)
    }).Select(this2 => new {
        this2 = this2,
        dateTicks = (new DateTime(this2.date.Year, this2.date.Month, this2.date.Day, this2.date.Hour, 0, 0)).Ticks
    }).Select(this3 => new {
        Hour = this3.dateTicks,
        State = this3.this2.state
    })
} into this0
select new {
    this0 = this0,
    stateGrouping = this0.hourlyGrouping.GroupBy(x0 => new {
        Hour = x0.Hour,
        Type = x0.State.Type
    }).Select(g => new {
        Type = g.Key.Type,
        StartTicks = g.Key.Hour,
        Collection = g.Select(x1 => new {
            Id = x1.State.Id,
            StartTicks = x1.State.StartTicks,
            Duration = x1.State.DurationTicks
        })
    })
} into this1
select new {
    Owner = this1.this0.activity.Owner,
    StartTick = DynamicEnumerable.Min(this1.stateGrouping, x2 => ((long)x2.StartTicks)),
    Building = this1.stateGrouping.Where(x3 => x3.Type == ""Building"").SelectMany(x4 => x4.Collection),
    Debugging = this1.stateGrouping.Where(x5 => x5.Type == ""Debugging"").SelectMany(x6 => x6.Collection),
    Coding = this1.stateGrouping.Where(x7 => x7.Type == ""Coding"").SelectMany(x8 => x8.Collection),
    System = this1.stateGrouping.Where(x9 => x9.Type == ""System"").SelectMany(x10 => x10.Collection)
}" },
                    Reduce = @"from result in results
group result by new {
    Owner = result.Owner,
    Hour = result.StartTick
} into g
select new {
    Owner = g.Key.Owner,
    StartTick = g.Key.Hour,
    Building = g.SelectMany(x => x.Building),
    Debugging = g.SelectMany(x0 => x0.Debugging),
    Coding = g.SelectMany(x1 => x1.Coding),
    System = g.SelectMany(x2 => x2.System)
}",
                    Fields =
                    {
                        {"__all_fields", new IndexFieldOptions { Storage =  FieldStorage.Yes} },
                        {"StartTick", new IndexFieldOptions {} }
                    }
                };
            }
        }
    }
}
