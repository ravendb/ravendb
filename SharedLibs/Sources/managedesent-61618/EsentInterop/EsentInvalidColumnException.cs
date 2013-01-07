//-----------------------------------------------------------------------
// <copyright file="EsentInvalidColumnException.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Exception thrown when a column conversion fails.
    /// </summary>
    [Serializable]
    public class EsentInvalidColumnException : EsentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidColumnException class.
        /// </summary>
        public EsentInvalidColumnException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidColumnException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentInvalidColumnException(SerializationInfo info, StreamingContext context) :
                base(info, context)
        {
        }

        /// <summary>
        /// Gets a text message describing the exception.
        /// </summary>
        public override string Message
        {
            get
            {
                return "Column is not valid for this operation";
            }
        }
    }
}
