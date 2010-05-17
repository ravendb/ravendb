//-----------------------------------------------------------------------
// <copyright file="jet_objectlist.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;

    /// <summary>
    /// The native version of the JET_OBJECTLIST structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1600:ElementsMustBeDocumented",
        Justification = "Internal interop struct only.")]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_OBJECTLIST
    {
        public uint cbStruct;
        public IntPtr tableid;
        public uint cRecord;
        public uint columnidcontainername;
        public uint columnidobjectname;
        public uint columnidobjtyp;
        public uint columniddtCreate;
        public uint columniddtUpdate;
        public uint columnidgrbit;
        public uint columnidflags;
        public uint columnidCollate;
        public uint columnidcRecord;
        public uint columnidcPage;
    }

    /// <summary>
    /// Information about a temporary table containing information
    /// about all tables for a given database.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_OBJECTLIST
    {
        /// <summary>
        /// Gets tableid of the temporary table. This should be closed
        /// when the table is no longer needed.
        /// </summary>
        public JET_TABLEID tableid { get; internal set; }

        /// <summary>
        /// Gets the number of records in the temporary table.
        /// </summary>
        public int cRecord { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the name of the table.
        /// </summary>
        public JET_COLUMNID columnidobjectname { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the type of the table.
        /// </summary>
        public JET_COLUMNID columnidobjtyp { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the grbits used when the table was created.
        /// </summary>
        public JET_COLUMNID columnidgrbit { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the table flags (e.g. the system table flag).
        /// </summary>
        public JET_COLUMNID columnidflags { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the number of records in the table.
        /// </summary>
        public JET_COLUMNID columnidcRecord { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the number of pages used by the table.
        /// </summary>
        public JET_COLUMNID columnidcPage { get; internal set; }

        /// <summary>
        /// Sets the fields of the object from a native JET_OBJECTLIST struct.
        /// </summary>
        /// <param name="value">
        /// The native objectlist to set the values from.
        /// </param>
        internal void SetFromNativeObjectlist(NATIVE_OBJECTLIST value)
        {
            this.tableid = new JET_TABLEID { Value = value.tableid };
            this.cRecord = checked((int) value.cRecord);

            this.columnidobjectname = new JET_COLUMNID { Value = value.columnidobjectname };
            this.columnidobjtyp = new JET_COLUMNID { Value = value.columnidobjtyp };
            this.columnidgrbit = new JET_COLUMNID { Value = value.columnidgrbit };
            this.columnidflags = new JET_COLUMNID { Value = value.columnidflags };
            this.columnidcRecord = new JET_COLUMNID { Value = value.columnidcRecord };
            this.columnidcPage = new JET_COLUMNID { Value = value.columnidcPage };
        }
    }
}