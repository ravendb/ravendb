using System.Collections.Generic;
using System.Linq;
using Tests.Infrastructure;

namespace Tests.ResourceSnapshotAggregator
{
    public static class SnapshotDelta
    {
        public static IReadOnlyList<ResourceUsageSnapshot> Calculate(IReadOnlyList<ResourceUsageSnapshot> snapshots)
        {
            var data = new List<ResourceUsageSnapshot>();
            var results = snapshots.Select((begin, i) =>
            {
                ResourceUsageSnapshot end = null;
                
                switch (begin.TestStage)
                {
                    case TestStage.TestAssemblyStarted:
                        end = snapshots.Skip(i).FirstOrDefault(x => x.TestStage == TestStage.TestAssemblyEnded);
                        break;
                    case TestStage.TestClassStarted:
                        end = snapshots.Skip(i).FirstOrDefault(x => x.TestStage == TestStage.TestClassEnded);
                        break;
                    case TestStage.TestStarted:
                        var end1 = snapshots.Skip(i).FirstOrDefault(x => x.TestStage == TestStage.TestEndedBeforeGc);
                        var end2 = snapshots.Skip(i).FirstOrDefault(x => x.TestStage == TestStage.TestEndedAfterGc);
                        
                        if(end1 != null && end2 != null)
                            return new[] { begin - end1, begin - end2 };
                        else if(end1 != null)
                            return new [] { begin - end1 };
                        else if(end2 != null)
                            return new [] { begin - end2 };
                        else
                            return new ResourceUsageSnapshot[0];
                }

                return end != null ? new [] { begin - end } : new ResourceUsageSnapshot[0];
            }).SelectMany(x => x);

            return results.ToList();
        }
    }
}
