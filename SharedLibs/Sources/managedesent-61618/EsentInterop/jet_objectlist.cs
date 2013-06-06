//-----------------------------------------------------------------------
// <copyright file="jet_objectlist.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_OBJECTLIST structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_OBJECTLIST
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Tableid of the temporary table.
        /// </summary>
        public IntPtr tableid;

        /// <summary>
        /// Number of records in the temporary table.
        /// </summary>
        public uint cRecord;

        /// <summary>
        /// The id of column containing the name of the container type.
        /// </summary>
        public uint columnidcontainername;

        /// <summary>
        /// The id of the column containing the name of the object.
        /// </summary>
        public uint columnidobjectname;

        /// <summary>
        /// The id of the column containing the type of the object.
        /// </summary>
        public uint columnidobjtyp;

        /// <summary>
        /// Obsolete. Do not use.
        /// </summary>
        [Obsolete("Unused member")]
        public uint columniddtCreate;

        /// <summary>
        /// Obsolete. Do not use.
        /// </summary>
        [Obsolete("Unused member")]
        public uint columniddtUpdate;

        /// <summary>
        /// The id of the column containing object grbits.
        /// </summary>
        public uint columnidgrbit;

        /// <summary>
        /// The id of the column containing object flags.
        /// </summary>
        public uint columnidflags;

        /// <summary>
        /// The id of the column containing the number of records in the table.
        /// </summary>
        public uint columnidcRecord;

        /// <summary>
        /// The id of the column containing the number of pages the object uses.
        /// </summary>
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
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_OBJECTLIST"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_OBJECTLIST"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_OBJECTLIST(0x{0:x},{1} records)",
                this.tableid,
                this.cRecord);
        }

        /// <summary>
        /// Sets the fields of the object from a native JET_OBJECTLIST struct.
        /// </summary>
        /// <param name="value">
        /// The native objectlist to set the values from.
        /// </param>
        internal void SetFromNativeObjectlist(NATIVE_OBJECTLIST value)
        {
            this.tableid = new JET_TABLEID { Value = value.tableid };
            this.cRecord = checked((int)value.cRecord);

            this.columnidobjectname = new JET_COLUMNID { Value = value.columnidobjectname };
            this.columnidobjtyp = new JET_COLUMNID { Value = value.columnidobjtyp };
            this.columnidgrbit = new JET_COLUMNID { Value = value.columnidgrbit };
            this.columnidflags = new JET_COLUMNID { Value = value.columnidflags };
            this.columnidcRecord = new JET_COLUMNID { Value = value.columnidcRecord };
            this.columnidcPage = new JET_COLUMNID { Value = value.columnidcPage };
        }
    }
}