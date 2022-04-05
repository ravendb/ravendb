using System;

namespace Raven.Server.Exceptions
{
    public class NotSupportedInShardingException : Exception
    {
        public NotSupportedInShardingException(string message) : base(message)
        {

        }
    }
}
