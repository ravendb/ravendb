//-----------------------------------------------------------------------
// <copyright file="EsentColumnNotFoundException.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Runtime.Serialization;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// The requested column isn't present in the table.
    /// </summary>
    [Serializable]
    public class EsentColumnNotFoundException : EsentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentColumnNotFoundException class.
        /// </summary>
        /// <param name="table">The table that was searched for the column.</param>
        /// <param name="column">The name of the column.</param>
        /// <param name="innerException">The exception raised when the column was searched for.</param>
        internal EsentColumnNotFoundException(string table, string column, Exception innerException)
            : base(String.Format("Column '{0}' is not present in the table '{1}'", column, table), innerException)
        {
            this.Data["table"] = table;
            this.Data["column"] = column;
        }

        /// <summary>
        /// Initializes a new instance of the EsentColumnNotFoundException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentColumnNotFoundException(SerializationInfo info, StreamingContext context) :
                base(info, context)
        {
        }
    }
}
