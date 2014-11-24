using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Spatial4n.Core.Exceptions;

namespace Raven.Abstractions.Exceptions
{
    public class InvalidSpatialShapeException : Exception
    {
        private readonly string invalidDocumentId;

        public InvalidSpatialShapeException(InvalidShapeException invalidShapeException, string invalidDocumentId)
            : base(invalidShapeException.Message, invalidShapeException)
        {
            this.invalidDocumentId = invalidDocumentId;
        }

        public string InvalidDocumentId
        {
            get { return invalidDocumentId; }
        }
    }
}
