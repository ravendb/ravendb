using System;

namespace Raven.Client.Documents.Exceptions
{
    public class DocumentParseException : Exception
    {
        public DocumentParseException(string id, Type toType)
            : base($"Could not parse document '{id}' to {toType.Name}.")
        {
        }

        public DocumentParseException(string id, Type toType, Exception innerException)
            : base($"Could not parse document '{id}' to {toType.Name}.", innerException)
        {
        }
    }
}