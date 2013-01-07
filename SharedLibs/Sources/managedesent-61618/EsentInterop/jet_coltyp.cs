//-----------------------------------------------------------------------
// <copyright file="jet_coltyp.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// ESENT column types.
    /// </summary>
    public enum JET_coltyp
    {
        /// <summary>
        /// Null column type. Invalid for column creation.
        /// </summary>
        Nil = 0,

        /// <summary>
        /// True, False or NULL.
        /// </summary>
        Bit = 1,

        /// <summary>
        /// 1-byte integer, unsigned.
        /// </summary>
        UnsignedByte = 2,

        /// <summary>
        /// 2-byte integer, signed.
        /// </summary>
        Short = 3,

        /// <summary>
        /// 4-byte integer, signed.
        /// </summary>
        Long = 4,

        /// <summary>
        /// 8-byte integer, signed.
        /// </summary>
        Currency = 5,

        /// <summary>
        /// 4-byte IEEE single-precisions.
        /// </summary>
        IEEESingle = 6,

        /// <summary>
        /// 8-byte IEEE double-precision.
        /// </summary>
        IEEEDouble = 7,

        /// <summary>
        /// Integral date, fractional time.
        /// </summary>
        DateTime = 8,

        /// <summary>
        /// Binary data, up to 255 bytes.
        /// </summary>
        Binary = 9,

        /// <summary>
        /// Text data, up to 255 bytes.
        /// </summary>
        Text = 10,

        /// <summary>
        /// Binary data, up to 2GB.
        /// </summary>
        LongBinary = 11,

        /// <summary>
        /// Text data, up to 2GB.
        /// </summary>
        LongText = 12,
    }
}