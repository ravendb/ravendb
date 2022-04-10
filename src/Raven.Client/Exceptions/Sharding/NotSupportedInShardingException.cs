using System;

namespace Raven.Client.Exceptions.Sharding
{
    public class NotSupportedInShardingException : RavenException
    {
        public NotSupportedInShardingException()
        {
        }

        public NotSupportedInShardingException(string message)
            : base(message)
        {
        }

        public NotSupportedInShardingException(string message, Exception e)
            : base(message, e)
        {
        }
    }
}
