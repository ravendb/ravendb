using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.Exceptions.Sharding;

public sealed class ExecutionForShardException : RavenException
{
    public ExecutionForShardException(int shardNumber, Exception inner)
        : base(GetMessage(shardNumber), inner)
    {
    }

    private static string GetMessage(int shardNumber)
    {
        return $"Shard {shardNumber} execution failed with an exception";
    }
}

