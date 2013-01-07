//-----------------------------------------------------------------------
// <copyright file="jet_columncreate.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_COLUMNCREATE structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_COLUMNCREATE
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Name of the column.
        /// </summary>
        public IntPtr szColumnName;

        /// <summary>
        /// Type of the columnn.
        /// </summary>
        public uint coltyp;

        /// <summary>
        /// The maximum length of this column (only relevant for binary and text columns).
        /// </summary>
        public uint cbMax;

        /// <summary>
        /// Column options.
        /// </summary>
        public uint grbit;

        /// <summary>
        /// Default value (NULL if none).
        /// </summary>
        public IntPtr pvDefault;

        /// <summary>
        /// Size of the default value.
        /// </summary>
        public uint cbDefault;

        /// <summary>
        /// Code page (for text columns only).
        /// </summary>
        public uint cp;

        /// <summary>
        /// The returned column id.
        /// </summary>
        public uint columnid;

        /// <summary>
        /// The returned error code.
        /// </summary>
        public int err;
    }

    /// <summary>
    /// Describes a column in a table of an ESENT database.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    [Serializable]
    public sealed class JET_COLUMNCREATE : IContentEquatable<JET_COLUMNCREATE>, IDeepCloneable<JET_COLUMNCREATE>
    {
        /// <summary>
        /// Name of the column.
        /// </summary>
        private string name;

        /// <summary>
        /// The type of the column.
        /// </summary>
        private JET_coltyp columnType;

        /// <summary>
        /// Maximum size of the column.
        /// </summary>
        private int maxSize;

        /// <summary>
        /// Column options.
        /// </summary>
        private ColumndefGrbit options;

        /// <summary>
        /// Default value (NULL if none).
        /// </summary>
        private byte[] defaultValue;

        /// <summary>
        /// Size of the default value.
        /// </summary>
        private int defaultValueSize;

        /// <summary>
        /// The code page. Only valid for text columns.
        /// </summary>
        private JET_CP codePage;

        /// <summary>
        /// Id of the column. Not serialized because it is an internal
        /// value and shouldn't be persisted.
        /// </summary>
        [NonSerialized]
        private JET_COLUMNID id;

        /// <summary>
        /// The returned error code.
        /// </summary>
        private JET_err errorCode;

        /// <summary>
        /// Gets or sets the name of the column to create. 
        /// </summary>
        public string szColumnName
        {
            [DebuggerStepThrough]
            get { return this.name; }
            set { this.name = value; }
        }

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
        /// Gets or sets the default value (NULL if none).
        /// </summary>
        public byte[] pvDefault
        {
            get { return this.defaultValue; }
            set { this.defaultValue = value; }
        }

        /// <summary>
        /// Gets or sets the size of the default value.
        /// </summary>
        public int cbDefault
        {
            get { return this.defaultValueSize; }
            set { this.defaultValueSize = value; }
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
        /// Gets the columnid of the column.
        /// </summary>
        public JET_COLUMNID columnid
        {
            [DebuggerStepThrough]
            get { return this.id; }
            internal set { this.id = value; }
        }

        /// <summary>
        /// Gets or sets the error code from creating this column.
        /// </summary>
        public JET_err err
        {
            [DebuggerStepThrough]
            get { return this.errorCode; }
            set { this.errorCode = value; }
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool ContentEquals(JET_COLUMNCREATE other)
        {
            if (null == other)
            {
                return false;
            }

            this.CheckMembersAreValid();
            other.CheckMembersAreValid();

            return this.err == other.err
                && this.szColumnName == other.szColumnName
                && this.coltyp == other.coltyp
                && this.cbMax == other.cbMax
                && this.grbit == other.grbit
                && this.cbDefault == other.cbDefault
                && this.cp == other.cp
                && this.columnid == other.columnid
                && Util.ArrayEqual(this.pvDefault, other.pvDefault, 0, other.cbDefault);
        }

        #region IDeepCloneable
        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        public JET_COLUMNCREATE DeepClone()
        {
            JET_COLUMNCREATE result = (JET_COLUMNCREATE)this.MemberwiseClone();
            if (this.pvDefault != null)
            {
                result.pvDefault = new byte[this.pvDefault.Length];
                Array.Copy(this.pvDefault, result.pvDefault, this.pvDefault.Length);
            }

            return result;
        }
        #endregion

        /// <summary>
        /// Generate a string representation of the instance.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_COLUMNCREATE({0},{1},{2})", this.szColumnName, this.coltyp, this.grbit);
        }

        /// <summary>
        /// Check this object to make sure its parameters are valid.
        /// </summary>
        internal void CheckMembersAreValid()
        {
            if (null == this.szColumnName)
            {
                throw new ArgumentNullException("szColumnName");
            }

            if (this.cbDefault < 0)
            {
                throw new ArgumentOutOfRangeException("cbDefault", this.cbDefault, "cannot be negative");
            }

            if (null == this.pvDefault && 0 != this.cbDefault)
            {
                throw new ArgumentOutOfRangeException("cbDefault", this.cbDefault, "must be 0");
            }

            if (null != this.pvDefault && (this.cbDefault > this.pvDefault.Length))
            {
                throw new ArgumentOutOfRangeException("cbDefault", this.cbDefault, "can't be greater than pvDefault.Length");
            }
        }

        /// <summary>
        /// Returns the unmanaged columncreate that represents this managed class.
        /// <see cref="szColumnName"/>, <see cref="pvDefault"/>, <see cref="columnid"/>,
        /// and <see cref="err"/> are not converted.
        /// </summary>
        /// <returns>A native (interop) version of the JET_COLUMNCREATE.</returns>
        internal NATIVE_COLUMNCREATE GetNativeColumnCreate()
        {
            var native = new NATIVE_COLUMNCREATE();
            native.cbStruct = checked((uint)Marshal.SizeOf(native));

            // columncreate.szColumnName is converted at pinvoke time.
            native.szColumnName = IntPtr.Zero;
            native.coltyp = (uint)this.coltyp;
            native.cbMax = (uint)this.cbMax;
            native.grbit = (uint)this.grbit;

            // columncreate.pvDefault is converted at pinvoke time.
            native.pvDefault = IntPtr.Zero;

            native.cbDefault = checked((uint)this.cbDefault);

            native.cp = (uint)this.cp;

            return native;
        }

        /// <summary>
        /// Sets only the output fields of the object from a native JET_COLUMNCREATE struct,
        /// specifically <see cref="columnid"/> and <see cref="err"/>.
        /// </summary>
        /// <param name="value">
        /// The native columncreate to set the values from.
        /// </param>
        internal void SetFromNativeColumnCreate(NATIVE_COLUMNCREATE value)
        {
            this.columnid = new JET_COLUMNID { Value = value.columnid };
            this.err = (JET_err)value.err;
        }
    }
}