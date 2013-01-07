//-----------------------------------------------------------------------
// <copyright file="EsentInvalidConversionException.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Runtime.Serialization;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// An object can't be converted to the right format.
    /// </summary>
    [Serializable]
    public class EsentInvalidConversionException : EsentException
    {
        /// <summary>
        /// Initializes a new instance of the EsentInvalidConversionException class.
        /// </summary>
        /// <param name="tablename">The name of the table the column wasn't found in.</param>
        /// <param name="columnName">The name of the column.</param>
        /// <param name="columnType">The type of column data was being converted for.</param>
        /// <param name="objectType">The type of object that the conversion failed for.</param>
        /// <param name="innerException">The exception raised during conversion.</param>
        internal EsentInvalidConversionException(string tablename, string columnName, ColumnType columnType, Type objectType, Exception innerException)
            : base(String.Format("Unable to convert an object of type {0} for column '{1}' ({2}) of table '{3}'", objectType, columnName, columnType, tablename), innerException)
        {
            this.Data["table"] = tablename;
            this.Data["column"] = columnName;
            this.Data["columnType"] = columnType;
            this.Data["objectType"] = objectType;
        }

        /// <summary>
        /// Initializes a new instance of the EsentInvalidConversionException class. This constructor
        /// is used to deserialize a serialized exception.
        /// </summary>
        /// <param name="info">The data needed to deserialize the object.</param>
        /// <param name="context">The deserialization context.</param>
        protected EsentInvalidConversionException(SerializationInfo info, StreamingContext context) :
                base(info, context)
        {
        }
    }
}
