using System;

namespace Voron.Exceptions
{
    public class HardLinkLimitExceededException : VoronErrorException
    {
        public HardLinkLimitExceededException(string message) : base(message)
        {
        }

        public HardLinkLimitExceededException(string message, Exception inner) : base(message, inner)
        {
        }
    }
}
