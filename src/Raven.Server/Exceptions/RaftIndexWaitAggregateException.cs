using System;
using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Exceptions;

public class RaftIndexWaitAggregateException : AggregateException
{
    public RaftIndexWaitAggregateException(long raftCommandIndex, IEnumerable<Exception> innerExceptions)
        : base(CreateMessage(raftCommandIndex, innerExceptions), innerExceptions)
    {
    }

    private static string CreateMessage(long raftCommandIndex, IEnumerable<Exception> innerExceptions)
    {
        var count = innerExceptions?.Count() ?? 0;

        return count switch
        {
            0 => $"An error was encountered during the execution of the raft command (number '{raftCommandIndex}'), but no specific exceptions were aggregated.",
            1 => $"An error occurred while executing the raft command (number '{raftCommandIndex}'). There is one exception aggregated within this exception.",
            _ => $"Multiple errors ({count} in total) occurred while executing the raft command (number '{raftCommandIndex}'). These errors are aggregated within this exception."
        };
    }
}
