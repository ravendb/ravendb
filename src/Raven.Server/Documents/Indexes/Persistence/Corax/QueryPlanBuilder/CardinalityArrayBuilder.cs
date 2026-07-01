using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Corax.Querying.Planning;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal static class CardinalityArrayBuilder
{
    public static (int[] InRangeCounts, long[] Cardinalities) Build(List<ClauseExecution> executions, bool isAllEntries)
    {
        List<int> inRange = [];
        List<long> cards = [];
        if (isAllEntries)
            cards.Add(0); // reserve slot 0 for the synthetic AllEntries match

        foreach (ClauseExecution exec in executions)
        {
            Walk(exec);
        }

        var inRangeCounts = inRange.Count == 0 ? [] : inRange.ToArray();
        var cardinalities = cards.Count == 0 ? null : cards.ToArray();
        return (inRangeCounts, cardinalities);

        void Walk(ClauseExecution exec)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            switch (exec.ClauseType)
            {
                case ClauseType.MatchAll:
                case ClauseType.MatchNothing:
                    break; // a collapse sentinel emits no match leaf → it consumes no cardinality slot

                case ClauseType.OrGroup:
                case ClauseType.AndGroup:
                    foreach (ClauseExecution sub in exec.SubExecutions)
                    {
                        Walk(sub);
                    }
                    break;

                case ClauseType.In:
                case ClauseType.AllIn:
                    inRange.Add(exec.InTermCount);
                    int n = exec.InTermCount + 1;
                    for (int i = 0; i < n; i++)
                    {
                        cards.Add(exec.Cardinality);
                    }

                    break;

                default:
                    cards.Add(exec.Cardinality);
                    break;
            }
        }
    }
}
