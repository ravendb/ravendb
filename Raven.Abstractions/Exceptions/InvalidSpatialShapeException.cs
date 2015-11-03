using System;
using System.Runtime.Serialization;

namespace Raven.Abstractions.Exceptions
{

    [Serializable]
    public class InvalidSpatialShapException : Exception
    {


        public InvalidSpatialShapException()
        {
        }

        public InvalidSpatialShapException(string message)
            : base(message)
        {
        }

        public InvalidSpatialShapException(string message, Exception inner)
            : base(message, inner)
        {
        }

        private readonly string invalidDocumentId;

        public InvalidSpatialShapException(Exception invalidShapeException, string invalidDocumentId)
            : base(invalidShapeException.Message, invalidShapeException)
        {
            this.invalidDocumentId = invalidDocumentId;
        }

        public string InvalidDocumentId
        {
            get { return invalidDocumentId; }
        }

        protected InvalidSpatialShapException(
            SerializationInfo info,
            StreamingContext context)
            : base(info, context)
        {
        }
    }
}
