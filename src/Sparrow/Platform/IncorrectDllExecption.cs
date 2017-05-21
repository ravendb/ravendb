using System;

namespace Sparrow.Platform
{
    public class IncorrectDllExecption : Exception
    {
        public IncorrectDllExecption()
        {
        }

        public IncorrectDllExecption(string message) : base(message)
        {
        }

        public IncorrectDllExecption(string message, Exception innerException) : base(message, innerException)
        {
        }
    }
}