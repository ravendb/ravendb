//-----------------------------------------------------------------------
// <copyright file="jet_columnlist.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_COLUMNLIST structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_COLUMNLIST
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
        /// Columnid of the presentation order column.
        /// </summary>
        public uint columnidPresentationOrder;

        /// <summary>
        /// Columnid of the name column.
        /// </summary>
        public uint columnidcolumnname;

        /// <summary>
        /// Columnid of the columnid column.
        /// </summary>
        public uint columnidcolumnid;

        /// <summary>
        /// Columnid of the coltyp column.
        /// </summary>
        public uint columnidcoltyp;

        /// <summary>
        /// Columnid of the country column.
        /// </summary>
        public uint columnidCountry;

        /// <summary>
        /// Columnid of the langid column.
        /// </summary>
        public uint columnidLangid;

        /// <summary>
        /// Columnid of the codepage column.
        /// </summary>
        public uint columnidCp;

        /// <summary>
        /// Columnid of the collation column.
        /// </summary>
        public uint columnidCollate;

        /// <summary>
        /// Columnid of the cbMax column.
        /// </summary>
        public uint columnidcbMax;

        /// <summary>
        /// Columnid of the grbit column.
        /// </summary>
        public uint columnidgrbit;

        /// <summary>
        /// Columnid of the default value column.
        /// </summary>
        public uint columnidDefault;

        /// <summary>
        /// Columnid of the base table name column.
        /// </summary>
        public uint columnidBaseTableName;

        /// <summary>
        /// Columnid of the base column name column.
        /// </summary>
        public uint columnidBaseColumnName;

        /// <summary>
        /// The column identifier of the name of the column definition.
        /// </summary>
        public uint columnidDefinitionName;
    }

    /// <summary>
    /// Information about a temporary table containing information
    /// about all columns for a given table.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_COLUMNLIST
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
        /// stores the name of the column.
        /// </summary>
        public JET_COLUMNID columnidcolumnname { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the id of the column.
        /// </summary>
        public JET_COLUMNID columnidcolumnid { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the type of the column.
        /// </summary>
        public JET_COLUMNID columnidcoltyp { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the code page of the column.
        /// </summary>
        public JET_COLUMNID columnidCp { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the maximum length of the column.
        /// </summary>
        public JET_COLUMNID columnidcbMax { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the grbit of the column.
        /// </summary>
        public JET_COLUMNID columnidgrbit { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the default value of the column.
        /// </summary>
        public JET_COLUMNID columnidDefault { get; internal set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_COLUMNLIST"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_COLUMNLIST"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_COLUMNLIST(0x{0:x},{1} records)",
                this.tableid,
                this.cRecord);
        }

        /// <summary>
        /// Sets the fields of the object from a native JET_COLUMNLIST struct.
        /// </summary>
        /// <param name="value">
        /// The native columnlist to set the values from.
        /// </param>
        internal void SetFromNativeColumnlist(NATIVE_COLUMNLIST value)
        {
            this.tableid = new JET_TABLEID { Value = value.tableid };
            this.cRecord = checked((int)value.cRecord);
            this.columnidcolumnname = new JET_COLUMNID { Value = value.columnidcolumnname };
            this.columnidcolumnid = new JET_COLUMNID { Value = value.columnidcolumnid };
            this.columnidcoltyp = new JET_COLUMNID { Value = value.columnidcoltyp };
            this.columnidCp = new JET_COLUMNID { Value = value.columnidCp };
            this.columnidcbMax = new JET_COLUMNID { Value = value.columnidcbMax };
            this.columnidgrbit = new JET_COLUMNID { Value = value.columnidgrbit };
            this.columnidDefault = new JET_COLUMNID { Value = value.columnidDefault };
        }
    }
}