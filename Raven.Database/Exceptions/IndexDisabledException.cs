using System;
using System.Runtime.Serialization;
using Raven.Database.Data;

namespace Raven.Database.Exceptions
{
    [Serializable]
    public class IndexDisabledException : Exception
    {
        public IndexFailureInformation Information { get; set; }

        public IndexDisabledException()
        {
        }

        public IndexDisabledException(IndexFailureInformation information)
        {
            Information = information;
        }

        public IndexDisabledException(string message) : base(message)
        {
        }

        public IndexDisabledException(string message, Exception inner) : base(message, inner)
        {
        }

        protected IndexDisabledException(
            SerializationInfo info,
            StreamingContext context) : base(info, context)
        {
        }
    }
}