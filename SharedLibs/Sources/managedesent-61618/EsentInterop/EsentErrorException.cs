//-----------------------------------------------------------------------
// <copyright file="EsentErrorException.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Base class for ESENT error exceptions.
    /// </summary>
    [Serializable]
    public class EsentErrorException : EsentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentErrorException class.
        /// </summary>
        /// <param name="message">The description of the error.</param>
        /// <param name="err">The error code of the exception.</param>
        internal EsentErrorException(string message, JET_err err) : base(message)
        {
            this.Data["error"] = err;
        }

        /// <summary>
        /// Initializes a new instance of the EsentErrorException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentErrorException(SerializationInfo info, StreamingContext context) :
                base(info, context)
        {
        }

        /// <summary>
        /// Gets the underlying Esent error for this exception.
        /// </summary>
        public JET_err Error
        {
            get
            {
                return (JET_err)this.Data["error"];
            }
        }
    }
}
