//-----------------------------------------------------------------------
// <copyright file="jet_recordlist.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Runtime.InteropServices;

    /// <summary>
    /// The native version of the JET_RECORDLIST structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_RECORDLIST
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Temporary table containing the bookmarks.
        /// </summary>
        public IntPtr tableid;

        /// <summary>
        /// Number of records in the table.
        /// </summary>
        public uint cRecords;

        /// <summary>
        /// Column id of the column containing the record bookmarks.
        /// </summary>
        public uint columnidBookmark;
    }

    /// <summary>
    /// Information about a temporary table containing information
    /// about all indexes for a given table.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_RECORDLIST
    {
        /// <summary>
        /// Gets tableid of the temporary table. This should be closed
        /// when the table is no longer needed.
        /// </summary>
        public JET_TABLEID tableid { get; internal set; }

        /// <summary>
        /// Gets the number of records in the temporary table.
        /// </summary>
        public int cRecords { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the bookmark of the record.
        /// The column is of type JET_coltyp.Text.
        /// </summary>
        public JET_COLUMNID columnidBookmark { get; internal set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_INDEXLIST"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_INDEXLIST"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_RECORDLIST(0x{0:x},{1} records)",
                this.tableid,
                this.cRecords);
        }

        /// <summary>
        /// Sets the fields of the object from a native JET_RECORDLIST struct.
        /// </summary>
        /// <param name="value">
        /// The native recordlist to set the values from.
        /// </param>
        internal void SetFromNativeRecordlist(NATIVE_RECORDLIST value)
        {
            this.tableid = new JET_TABLEID { Value = value.tableid };
            this.cRecords = checked((int)value.cRecords);
            this.columnidBookmark = new JET_COLUMNID { Value = value.columnidBookmark };
        }
    }
}

