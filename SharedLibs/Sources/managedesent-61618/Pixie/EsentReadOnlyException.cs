//-----------------------------------------------------------------------
// <copyright file="EsentReadOnlyException.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Runtime.Serialization;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// The write operation tried on a read-only connection.
    /// </summary>
    [Serializable]
    public class EsentReadOnlyException : EsentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentReadOnlyException class.
        /// </summary>
        /// <param name="name">
        /// The name of the object that the modification was attempted on.
        /// </param>
        internal EsentReadOnlyException(string name)
            : base(String.Format("Write operation attempted on read-only object '{0}'", name))
        {
        }

        /// <summary>
        /// Initializes a new instance of the EsentReadOnlyException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentReadOnlyException(SerializationInfo info, StreamingContext context) :
                base(info, context)
        {
        }
    }
}
