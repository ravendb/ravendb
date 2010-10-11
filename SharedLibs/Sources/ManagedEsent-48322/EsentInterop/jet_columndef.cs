//-----------------------------------------------------------------------
// <copyright file="jet_columndef.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;

    /// <summary>
    /// The native version of the JET_COLUMNDEF structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_COLUMNDEF
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Column ID.
        /// </summary>
        public uint columnid;

        /// <summary>
        /// Type of the column.
        /// </summary>
        public uint coltyp;

        /// <summary>
        /// Reserved. Should be 0.
        /// </summary>
        public ushort wCountry;

        /// <summary>
        /// Obsolete. Should be 0.
        /// </summary>
        public ushort langid;

        /// <summary>
        /// Code page for text columns.
        /// </summary>
        public ushort cp;

        /// <summary>
        /// Reserved. Should be 0.
        /// </summary>
        public ushort wCollate;

        /// <summary>
        /// Maximum length of the column.
        /// </summary>
        public uint cbMax;

        /// <summary>
        /// Column options.
        /// </summary>
        public uint grbit;
    }

    /// <summary>
    /// Describes a column in a table of an ESENT database.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_COLUMNDEF
    {
        /// <summary>
        /// Gets or sets type of the column.
        /// </summary>
        public JET_coltyp coltyp { get; set; }

        /// <summary>
        /// Gets or sets code page of the column. This is only meaningful for columns of type
        /// <see cref="JET_coltyp.Text"/> and <see cref="JET_coltyp.LongText"/>.
        /// </summary>
        public JET_CP cp { get; set; }

        /// <summary>
        /// Gets or sets the maximum length of the column. This is only meaningful for columns of
        /// type <see cref="JET_coltyp.Text"/>, <see cref="JET_coltyp.LongText"/>, <see cref="JET_coltyp.Binary"/> and
        /// <see cref="JET_coltyp.LongBinary"/>.
        /// </summary>
        public int cbMax { get; set; }

        /// <summary>
        /// Gets or sets the column options.
        /// </summary>
        public ColumndefGrbit grbit { get; set; }

        /// <summary>
        /// Gets the columnid of the column.
        /// </summary>
        public JET_COLUMNID columnid { get; internal set; }

        /// <summary>
        /// Returns the unmanaged columndef that represents this managed class.
        /// </summary>
        /// <returns>A native (interop) version of the JET_COLUMNDEF.</returns>
        internal NATIVE_COLUMNDEF GetNativeColumndef()
        {
            var columndef = new NATIVE_COLUMNDEF();
            columndef.cbStruct = checked((uint)Marshal.SizeOf(columndef));
            columndef.cp = (ushort)this.cp;
            columndef.cbMax = checked((uint)this.cbMax);
            columndef.grbit = (uint)this.grbit;
            columndef.coltyp = checked((uint)this.coltyp);
            return columndef;
        }

        /// <summary>
        /// Sets the fields of the object from a native JET_COLUMNDEF struct.
        /// </summary>
        /// <param name="value">
        /// The native columndef to set the values from.
        /// </param>
        internal void SetFromNativeColumndef(NATIVE_COLUMNDEF value)
        {
            this.coltyp = (JET_coltyp)value.coltyp;
            this.cp = (JET_CP)value.cp;
            this.cbMax = checked((int)value.cbMax);
            this.grbit = (ColumndefGrbit)value.grbit;
            this.columnid = new JET_COLUMNID { Value = value.columnid };
        }
    }
}
