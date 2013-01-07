//-----------------------------------------------------------------------
// <copyright file="jet_columndef.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
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
        [Obsolete("Reserved")]
        public ushort wCountry;

        /// <summary>
        /// Obsolete. Should be 0.
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
    [Serializable]
    public sealed class JET_COLUMNDEF : IContentEquatable<JET_COLUMNDEF>, IDeepCloneable<JET_COLUMNDEF>
    {
        /// <summary>
        /// The type of the column.
        /// </summary>
        private JET_coltyp columnType;

        /// <summary>
        /// The code page. Only valid for text columns.
        /// </summary>
        private JET_CP codePage;

        /// <summary>
        /// Maximum size of the column.
        /// </summary>
        private int maxSize;

        /// <summary>
        /// Id of the column. Not serialized because it is an internal
        /// value and shouldn't be persisted.
        /// </summary>
        [NonSerialized]
        private JET_COLUMNID id;

        /// <summary>
        /// Column options.
        /// </summary>
        private ColumndefGrbit options;

        /// <summary>
        /// Gets or sets type of the column.
        /// </summary>
        public JET_coltyp coltyp
        {
            [DebuggerStepThrough]
            get { return this.columnType; }
            set { this.columnType = value; }
        }

        /// <summary>
        /// Gets or sets code page of the column. This is only meaningful for columns of type
        /// <see cref="JET_coltyp.Text"/> and <see cref="JET_coltyp.LongText"/>.
        /// </summary>
        public JET_CP cp
        {
            [DebuggerStepThrough]
            get { return this.codePage; }
            set { this.codePage = value; }
        }

        /// <summary>
        /// Gets or sets the maximum length of the column. This is only meaningful for columns of
        /// type <see cref="JET_coltyp.Text"/>, <see cref="JET_coltyp.LongText"/>, <see cref="JET_coltyp.Binary"/> and
        /// <see cref="JET_coltyp.LongBinary"/>.
        /// </summary>
        public int cbMax
        {
            [DebuggerStepThrough]
            get { return this.maxSize; }
            set { this.maxSize = value; }
        }

        /// <summary>
        /// Gets or sets the column options.
        /// </summary>
        public ColumndefGrbit grbit
        {
            [DebuggerStepThrough]
            get { return this.options; }
            set { this.options = value; }
        }

        /// <summary>
        /// Gets the columnid of the column.
        /// </summary>
        public JET_COLUMNID columnid
        {
            [DebuggerStepThrough]
            get { return this.id; }
            internal set { this.id = value; }
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_COLUMNDEF"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_COLUMNDEF"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_COLUMNDEF({0},{1})", this.columnType, this.options);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool ContentEquals(JET_COLUMNDEF other)
        {
            if (null == other)
            {
                return false;
            }

            return this.columnType == other.columnType
                   && this.codePage == other.codePage
                   && this.maxSize == other.maxSize
                   && this.id == other.id
                   && this.options == other.options;
        }

        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        public JET_COLUMNDEF DeepClone()
        {
            return (JET_COLUMNDEF)this.MemberwiseClone();
        }

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