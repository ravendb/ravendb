//-----------------------------------------------------------------------
// <copyright file="VistaColtyp.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop.Vista
{
    /// <summary>
    /// Column types that have been added to the Vista version of ESENT.
    /// </summary>
    public static class VistaColtyp
    {
        /// <summary>
        /// Unsigned 32-bit number.
        /// </summary>
        public const JET_coltyp UnsignedLong = (JET_coltyp)14;

        /// <summary>
        /// Signed 64-bit number.
        /// </summary>
        public const JET_coltyp LongLong = (JET_coltyp)15;

        /// <summary>
        /// 16-byte GUID.
        /// </summary>
        public const JET_coltyp GUID = (JET_coltyp)16;

        /// <summary>
        /// Unsigned 16-bit number.
        /// </summary>
        public const JET_coltyp UnsignedShort = (JET_coltyp)17;
    }
}