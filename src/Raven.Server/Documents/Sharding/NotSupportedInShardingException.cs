using System;

namespace Raven.Server.Documents.Sharding;

public class NotSupportedInShardingException : Exception
{
    public NotSupportedInShardingException()
    {
    }

    public NotSupportedInShardingException(string message) : base(message)
    {
    }

    public NotSupportedInShardingException(string message, Exception inner) : base(message, inner)
    {
    }
}
