using System;

namespace Raven.NewClient.Client.Exceptions
{
    public class DocumentParseException : Exception
    {
        public DocumentParseException(string key, Type toType)
            : base($"Could not parse document '{key}' to {toType.Name}.")
        {
        }

        public DocumentParseException(string key, Type toType, Exception innerException)
            : base($"Could not parse document '{key}' to {toType.Name}.", innerException)
        {
        }
    }
}