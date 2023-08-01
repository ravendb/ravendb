using System;

namespace Voron.Exceptions
{
    public sealed class InvalidFixedSizeTree : Exception
    {
        public InvalidFixedSizeTree()
        {
            
        }

        public InvalidFixedSizeTree(string message) : base(message)
        {
        }

        public InvalidFixedSizeTree(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
