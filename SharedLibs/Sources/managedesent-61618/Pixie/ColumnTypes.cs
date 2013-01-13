//-----------------------------------------------------------------------
// <copyright file="ColumnTypes.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent
{
    /// <summary>
    /// A column type.
    /// </summary>
    public enum ColumnType
    {
        /// <summary>
        /// Boolean: true/false (JET_coltypBit)
        /// </summary>
        Bool,

        /// <summary>
        /// Unsigned byte (JET_coltypUnsignedByte)
        /// </summary>
        Byte,

        /// <summary>
        /// 16-bit signed integer (JET_coltypShort)
        /// </summary>
        Int16,

        /// <summary>
        /// 16-bit unsigned integer (JET_coltypUnsignedShort)
        /// </summary>
        UInt16,

        /// <summary>
        /// 32-bit signed integer (JET_coltypLong)
        /// </summary>
        Int32,

        /// <summary>
        /// 32-bit unsigned integer (JET_coltypUnsignedLong)
        /// </summary>
        UInt32,

        /// <summary>
        /// 64-bit signed integer (JET_coltypCurrency)
        /// </summary>
        Int64,

        /// <summary>
        /// Unicode text (JET_coltypLongText)
        /// </summary>
        Text,

        /// <summary>
        /// ASCII text (JET_coltypLongText)
        /// </summary>
        AsciiText,

        /// <summary>
        /// Binary data (JET_coltypLongBinary)
        /// </summary>
        Binary,

        /// <summary>
        /// IEEE single (JET_coltypIEEESingle)
        /// </summary>
        Float,

        /// <summary>
        /// IEEE double (JET_coltypIEEEDouble)
        /// </summary>
        Double,

        /// <summary>
        /// DateTime (JET_coltypDateTime)
        /// </summary>
        DateTime,

        /// <summary>
        /// Guid (JET_coltypGuid)
        /// </summary>
        Guid,
    }
}