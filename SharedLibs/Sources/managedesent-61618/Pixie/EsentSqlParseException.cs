//-----------------------------------------------------------------------
// <copyright file="EsentSqlParseException.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Runtime.Serialization;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Parse exceptions.
    /// </summary>
    [Serializable]
    public class EsentSqlParseException : EsentException
    {
        /// <summary>
        /// Trace object used to track parse exceptions.
        /// </summary>
        private static readonly Tracer tracer = new Tracer("ParseException", "Parsing exception", "ParseException");

        /// <summary>
        /// Initializes a new instance of the EsentSqlParseException class.
        /// </summary>
        /// <param name="description">Description of the parse error.</param>
        internal EsentSqlParseException(string description)
            : base(description)
        {
            this.Tracer.TraceError(description);
        }

        /// <summary>
        /// Initializes a new instance of the EsentSqlParseException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentSqlParseException(SerializationInfo info, StreamingContext context) :
                base(info, context)
        {
        }

        /// <summary>
        /// Gets the Tracer object for the ParseException class.
        /// </summary>
        private Tracer Tracer
        {
            get
            {
                return EsentSqlParseException.tracer;
            }
        }
    }
}
