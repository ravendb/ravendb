//-----------------------------------------------------------------------
// <copyright file="EsentSqlExecutionException.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Runtime.Serialization;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Invalid SQL command.
    /// </summary>
    [Serializable]
    public class EsentSqlExecutionException : EsentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentSqlExecutionException class.
        /// </summary>
        /// <param name="description">
        /// Description of the error.
        /// </param>
        internal EsentSqlExecutionException(string description)
            : base(description)
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentSqlExecutionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentSqlExecutionException(SerializationInfo info, StreamingContext context) :
                base(info, context)
        {
        }
    }
}
