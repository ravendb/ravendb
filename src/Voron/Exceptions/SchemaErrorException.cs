using System;
using System.Runtime.ExceptionServices;

namespace Voron.Exceptions
{
    public class SchemaErrorException : Exception
    {
        public SchemaErrorException()
        {
        }

        public SchemaErrorException(string message) : base(message)
        {
        }

        public SchemaErrorException(string message, Exception inner) : base(message, inner)
        {
        }

        public static void Raise(StorageEnvironment env, string message)
        {
            try
            {
                throw new SchemaErrorException(message);
            }
            catch (Exception e)
            {
                env.Options.SetCatastrophicFailure(ExceptionDispatchInfo.Capture(e));
                throw;
            }
        }

    }
}
