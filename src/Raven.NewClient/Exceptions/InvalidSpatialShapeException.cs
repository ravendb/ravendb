using System;

namespace Raven.NewClient.Client.Exceptions
{
    public class InvalidSpatialShapeException : Exception
    {
        public InvalidSpatialShapeException()
        {
        }

        public InvalidSpatialShapeException(string message)
            : base(message)
        {
        }

        public InvalidSpatialShapeException(string message, Exception inner)
            : base(message, inner)
        {
        }

        public InvalidSpatialShapeException(Exception invalidShapeException, string invalidDocumentId)
            : base(invalidShapeException.Message, invalidShapeException)
        {
            InvalidDocumentId = invalidDocumentId;
        }

        public string InvalidDocumentId { get; }
    }
}
