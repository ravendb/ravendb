#if !SILVERLIGHT
//this is server-side related, so we do not need this in silverlight

using System;
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

#endif