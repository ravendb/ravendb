//-----------------------------------------------------------------------
// <copyright file="ColumnMetaData.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

using System;
using Microsoft.Isam.Esent.Interop;

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// Information about an ESE column.
    /// </summary>
    public class ColumnMetaData
    {
        /// <summary>
        /// Gets the name of the column.
        /// </summary>
        public string Name { get; internal set; }

        /// <summary>
        /// Gets the type of the column.
        /// </summary>
        public ColumnType Type { get; internal set; }

        /// <summary>
        /// Gets a value indicating whether the column is an autoincrement column.
        /// </summary>
        public bool IsAutoincrement { get; internal set; }

        /// <summary>
        /// Gets a value indicating whether the column is a non-NULL column. If this
        /// property is true then the column can never be null.
        /// </summary>
        public bool IsNotNull { get; internal set; }

        /// <summary>
        /// Gets a value indicating whether the column is a version column.
        /// </summary>
        public bool IsVersion { get; internal set; }

        /// <summary>
        /// Gets a value indicating whether the column is an escrow-update column.
        /// </summary>
        public bool IsEscrowUpdate { get; internal set; }

        /// <summary>
        /// Gets a value giving the maximum size of the column, in bytes.
        /// </summary>
        public int MaxSize { get; internal set; }

        /// <summary>
        /// Gets a value specifying the default value of the column.
        /// </summary>
        public object DefaultValue { get; internal set; }

        /// <summary>
        /// Gets a value indicating whether the column
        /// is indexed.
        /// </summary>
        public bool IsIndexed { get; internal set; }

        /// <summary>
        /// Gets a value indicating whether the column
        /// is unique. A column is unique if it is the only
        /// column in a unique index definition.
        /// </summary>
        public bool IsUnique { get; internal set; }

        /// <summary>
        /// Gets or sets the columnid of the column.
        /// </summary>
        internal JET_COLUMNID Columnid { get; set; }

        /// <summary>
        /// Gets or setsa function that converts an object into the appropriate
        /// object type for this column.
        /// </summary>
        internal Converter<object, object> ObjectConverter { get; set; }

        /// <summary>
        /// Gets or sets a function that converts an object into a byte array.
        /// An object can be converted to the appropriate form with 
        /// the ObjectConverter function.
        /// </summary>
        internal Converter<object, byte[]> ObjectToBytesConverter { get; set; }

        /// <summary>
        /// Gets or sets a function that converts an byte array to an
        /// object for the column.
        /// </summary>
        internal Converter<byte[], object> BytesToObjectConverter { get; set; }

        /// <summary>
        /// Gets or sets a function to set the column.
        /// </summary>
        internal Action<Cursor, object> SetColumn { get; set; }

        /// <summary>
        /// Gets or sets a function to make a key for the column.
        /// </summary>
        internal Action<Cursor, object, MakeKeyGrbit> MakeKey { get; set; }

        /// <summary>
        /// Gets or sets a function to retrieve the column.
        /// </summary>
        internal Func<Cursor, RetrieveColumnGrbit, object> RetrieveColumn { get; set; }
    }
}