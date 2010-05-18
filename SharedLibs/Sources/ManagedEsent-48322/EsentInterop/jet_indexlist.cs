//-----------------------------------------------------------------------
// <copyright file="jet_indexlist.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;

    /// <summary>
    /// The native version of the JET_INDEXLIST structure.
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
    internal struct NATIVE_INDEXLIST
    {
        public uint cbStruct;
        public IntPtr tableid;
        public uint cRecord;
        public uint columnidindexname;
        public uint columnidgrbitIndex;
        public uint columnidcKey;
        public uint columnidcEntry;
        public uint columnidcPage;
        public uint columnidcColumn;
        public uint columnidiColumn;
        public uint columnidcolumnid;
        public uint columnidcoltyp;
        public uint columnidCountry;
        public uint columnidLangid;
        public uint columnidCp;
        public uint columnidCollate;
        public uint columnidgrbitColumn;
        public uint columnidcolumnname;
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
    public class JET_INDEXLIST
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