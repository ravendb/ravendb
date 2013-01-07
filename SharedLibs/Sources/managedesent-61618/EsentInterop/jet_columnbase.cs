//-----------------------------------------------------------------------
// <copyright file="jet_columnbase.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_COLUMNBASE structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, CharSet = CharSet.Ansi)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_COLUMNBASE
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
        [Obsolete("Reserved")]
        public ushort wCountry;

        /// <summary>
        /// Obsolete. Should be .
        /// </summary>
        [Obsolete("Use cp")]
        public ushort langid;

        /// <summary>
        /// Code page for text columns.
        /// </summary>
        public ushort cp;

        /// <summary>
        /// Reserved. Should be 0.
        /// </summary>
        [Obsolete("Reserved")]
        public ushort wFiller;

        /// <summary>
        /// Maximum length of the column.
        /// </summary>
        public uint cbMax;

        /// <summary>
        /// Column options.
        /// </summary>
        public uint grbit;

        /// <summary>
        /// The table from which the current table inherits its DDL.
        /// </summary>
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = NameSize)]
        public string szBaseTableName;

        /// <summary>
        /// The name of the column in the template table.
        /// </summary>
        [MarshalAs(UnmanagedType.ByValTStr, SizeConst = NameSize)]
        public string szBaseColumnName;

        /// <summary>
        /// Max size of the table/column name string.
        /// </summary>
        private const int NameSize = 256;
    }

    /// <summary>
    /// Describes a column in a table of an ESENT database.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public sealed class JET_COLUMNBASE : IEquatable<JET_COLUMNBASE>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="JET_COLUMNBASE"/> class.
        /// </summary>
        internal JET_COLUMNBASE()
        {            
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="JET_COLUMNBASE"/> class.
        /// </summary>
        /// <param name="value">
        /// The value.
        /// </param>
        internal JET_COLUMNBASE(NATIVE_COLUMNBASE value)
        {
            this.coltyp = (JET_coltyp)value.coltyp;
            this.cp = (JET_CP)value.cp;
            this.cbMax = checked((int)value.cbMax);
            this.grbit = (ColumndefGrbit)value.grbit;
            this.columnid = new JET_COLUMNID { Value = value.columnid };
            this.szBaseTableName = StringCache.TryToIntern(value.szBaseTableName);
            this.szBaseColumnName = StringCache.TryToIntern(value.szBaseColumnName);
        }

        /// <summary>
        /// Gets type of the column.
        /// </summary>
        public JET_coltyp coltyp { get; internal set; }

        /// <summary>
        /// Gets code page of the column. This is only meaningful for columns of type
        /// <see cref="JET_coltyp.Text"/> and <see cref="JET_coltyp.LongText"/>.
        /// </summary>
        public JET_CP cp { get; internal set; }

        /// <summary>
        /// Gets the maximum length of the column. This is only meaningful for columns of
        /// type <see cref="JET_coltyp.Text"/>, <see cref="JET_coltyp.LongText"/>, <see cref="JET_coltyp.Binary"/> and
        /// <see cref="JET_coltyp.LongBinary"/>.
        /// </summary>
        public int cbMax { get; internal set; }

        /// <summary>
        /// Gets the column options.
        /// </summary>
        public ColumndefGrbit grbit { get; internal set; }

        /// <summary>
        /// Gets the columnid of the column.
        /// </summary>
        public JET_COLUMNID columnid { get; internal set; }

        /// <summary>
        /// Gets the table from which the current table inherits its DDL.
        /// </summary>
        public string szBaseTableName { get; internal set; }

        /// <summary>
        /// Gets the name of the column in the template table.
        /// </summary>
        public string szBaseColumnName { get; internal set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_COLUMNBASE"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_COLUMNBASE"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_COLUMNBASE({0},{1})", this.coltyp, this.grbit);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            var hashes = new[]
            {
                this.coltyp.GetHashCode(),
                this.cp.GetHashCode(),
                this.cbMax,
                this.grbit.GetHashCode(),
                this.columnid.GetHashCode(),
                this.szBaseTableName.GetHashCode(),
                this.szBaseColumnName.GetHashCode(),
            };

            return Util.CalculateHashCode(hashes);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_COLUMNBASE)obj);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_COLUMNBASE other)
        {
            if (null == other)
            {
                return false;
            }

            return this.coltyp == other.coltyp
                   && this.cp == other.cp
                   && this.cbMax == other.cbMax
                   && this.columnid == other.columnid
                   && this.grbit == other.grbit
                   && String.Equals(this.szBaseTableName, other.szBaseTableName, StringComparison.Ordinal)
                   && String.Equals(this.szBaseColumnName, other.szBaseColumnName, StringComparison.Ordinal);
        }
    }
}