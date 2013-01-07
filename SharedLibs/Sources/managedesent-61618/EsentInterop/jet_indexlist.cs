//-----------------------------------------------------------------------
// <copyright file="jet_indexlist.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_INDEXLIST structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_INDEXLIST
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
        /// Number of records in the table.
        /// </summary>
        public uint cRecord;

        /// <summary>
        /// Id of the column containing the name of the index.
        /// </summary>
        public uint columnidindexname;

        /// <summary>
        /// Id of the column containing index options.
        /// </summary>
        public uint columnidgrbitIndex;

        /// <summary>
        /// Id of the column containing the number of unique keys in the index.
        /// This is updated by <see cref="Api.JetComputeStats"/>.
        /// </summary>
        public uint columnidcKey;

        /// <summary>
        /// Id of the column containing the number of entries in the index.
        /// This is updated by <see cref="Api.JetComputeStats"/>.
        /// </summary>
        public uint columnidcEntry;

        /// <summary>
        /// Id of the column containing the number of pages in the index.
        /// This is updated by <see cref="Api.JetComputeStats"/>.
        /// </summary>
        public uint columnidcPage;

        /// <summary>
        /// Id of the column containing the number of columns in the index
        /// definition.
        /// </summary>
        public uint columnidcColumn;

        /// <summary>
        /// Id of the column storing the index of this column in the index key.
        /// </summary>
        public uint columnidiColumn;

        /// <summary>
        /// Id of the column containing the columnid.
        /// </summary>
        public uint columnidcolumnid;

        /// <summary>
        /// Id of the column containing the column type.
        /// </summary>
        public uint columnidcoltyp;

        /// <summary>
        /// Id of the column containing the country code (obsolete).
        /// </summary>
        [Obsolete("Deprecated")]
        public uint columnidCountry;

        /// <summary>
        /// Id of the column containing the LCID of the index.
        /// </summary>
        public uint columnidLangid;

        /// <summary>
        /// Id of the column containing the code page of the index.
        /// </summary>
        public uint columnidCp;

        /// <summary>
        /// Obsolete. Ignored.
        /// </summary>
        [Obsolete("Deprecated")]
        public uint columnidCollate;

        /// <summary>
        /// Id of the column giving the column options.
        /// </summary>
        public uint columnidgrbitColumn;

        /// <summary>
        /// Id of the column giving the column name.
        /// </summary>
        public uint columnidcolumnname;

        /// <summary>
        /// Id of the column giving the LCMapString options.
        /// </summary>
        public uint columnidLCMapFlags;
    }

    /// <summary>
    /// Information about a temporary table containing information
    /// about all indexes for a given table.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public sealed class JET_INDEXLIST
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
        /// stores the name of the index.
        /// The column is of type JET_coltyp.Text.
        /// </summary>
        public JET_COLUMNID columnidindexname { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the the grbits used on the index. See <see cref="CreateIndexGrbit"/>.
        /// The column is of type JET_coltyp.Long.
        /// </summary>
        public JET_COLUMNID columnidgrbitIndex { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the number of unique keys in the index.
        /// This value is not current and is only is updated by <see cref="Api.JetComputeStats"/>.
        /// The column is of type JET_coltyp.Long.
        /// </summary>
        public JET_COLUMNID columnidcKey { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the number of entries in the index.
        /// This value is not current and is only is updated by <see cref="Api.JetComputeStats"/>.
        /// The column is of type JET_coltyp.Long.
        /// </summary>
        public JET_COLUMNID columnidcEntry { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the number of pages in the index.
        /// This value is not current and is only is updated by <see cref="Api.JetComputeStats"/>.
        /// The column is of type JET_coltyp.Long.
        /// </summary>
        public JET_COLUMNID columnidcPage { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the number of columns in the index key.
        /// The column is of type JET_coltyp.Long.
        /// </summary>
        public JET_COLUMNID columnidcColumn { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the index of of this column in the index key.
        /// The column is of type JET_coltyp.Long.
        /// </summary>
        public JET_COLUMNID columnidiColumn { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the columnid of the column being indexed.
        /// The column is of type JET_coltyp.Long.
        /// </summary>
        public JET_COLUMNID columnidcolumnid { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the column type of the column being indexed.
        /// The column is of type JET_coltyp.Long.
        /// </summary>
        public JET_COLUMNID columnidcoltyp { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the language id (LCID) of the index.
        /// The column is of type JET_coltyp.Short.
        /// </summary>
        public JET_COLUMNID columnidLangid { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the code page of the indexed column.
        /// The column is of type JET_coltyp.Short.
        /// </summary>
        public JET_COLUMNID columnidCp { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the grbit that apply to the indexed column. See <see cref="IndexKeyGrbit"/>.
        /// The column is of type JET_coltyp.Long.
        /// </summary>
        public JET_COLUMNID columnidgrbitColumn { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the grbit that apply to the indexed column. See <see cref="IndexKeyGrbit"/>.
        /// The column is of type JET_coltyp.Text.
        /// </summary>
        public JET_COLUMNID columnidcolumnname { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column in the temporary table which
        /// stores the unicode normalization flags for the index.
        /// The column is of type JET_coltyp.Long.
        /// </summary>
        public JET_COLUMNID columnidLCMapFlags { get; internal set; }

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
                "JET_INDEXLIST(0x{0:x},{1} records)",
                this.tableid,
                this.cRecord);
        }

        /// <summary>
        /// Sets the fields of the object from a native JET_INDEXLIST struct.
        /// </summary>
        /// <param name="value">
        /// The native indexlist to set the values from.
        /// </param>
        internal void SetFromNativeIndexlist(NATIVE_INDEXLIST value)
        {
            this.tableid = new JET_TABLEID { Value = value.tableid };
            this.cRecord = checked((int)value.cRecord);
            this.columnidindexname = new JET_COLUMNID { Value = value.columnidindexname };
            this.columnidgrbitIndex = new JET_COLUMNID { Value = value.columnidgrbitIndex };
            this.columnidcKey = new JET_COLUMNID { Value = value.columnidcKey };
            this.columnidcEntry = new JET_COLUMNID { Value = value.columnidcEntry };
            this.columnidcPage = new JET_COLUMNID { Value = value.columnidcPage };
            this.columnidcColumn = new JET_COLUMNID { Value = value.columnidcColumn };
            this.columnidiColumn = new JET_COLUMNID { Value = value.columnidiColumn };
            this.columnidcolumnid = new JET_COLUMNID { Value = value.columnidcolumnid };
            this.columnidcoltyp = new JET_COLUMNID { Value = value.columnidcoltyp };
            this.columnidLangid = new JET_COLUMNID { Value = value.columnidLangid };
            this.columnidCp = new JET_COLUMNID { Value = value.columnidCp };
            this.columnidgrbitColumn = new JET_COLUMNID { Value = value.columnidgrbitColumn };
            this.columnidcolumnname = new JET_COLUMNID { Value = value.columnidcolumnname };
            this.columnidLCMapFlags = new JET_COLUMNID { Value = value.columnidLCMapFlags };
        }
    }
}