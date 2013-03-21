//-----------------------------------------------------------------------
// <copyright file="jet_enumcolumn.cs" company="Microsoft Corporation">
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
    /// Native (unmanaged) version of the JET_ENUMCOLUMN structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal unsafe struct NATIVE_ENUMCOLUMN
    {
        /// <summary>
        /// The columnid that was enumerated.
        /// </summary>
        public uint columnid;

        /// <summary>
        /// The column status code from the enumeration of the column.
        /// </summary>
        public int err;

        /// <summary>
        /// The size of the value that was enumerated for the column.
        /// This member is only used if <see cref="err"/> is equal to
        /// <see cref="JET_wrn.ColumnSingleValue"/>.
        /// </summary>
        /// <remarks>
        /// The unmanaged JET_ENUMCOLUMN structure is a union so this
        /// is aliased with cEnumColumnValue.
        /// </remarks>
        public uint cbData;

        /// <summary>
        /// The the value that was enumerated for the column.
        /// This member is only used if <see cref="err"/> is equal to
        /// <see cref="JET_wrn.ColumnSingleValue"/>.
        /// </summary>
        /// <remarks>
        /// The unmanaged JET_ENUMCOLUMN structure is a union so this
        /// is aliased with rgEnumColumnValue.
        /// </remarks>
        public IntPtr pvData;

        /// <summary>
        /// Gets or sets the number of entries in rgEnumColumnValue.
        /// This member is only used if <see cref="err"/> is not
        /// <see cref="JET_wrn.ColumnSingleValue"/>.
        /// </summary>
        /// <remarks>
        /// The unmanaged JET_ENUMCOLUMN structure is a union so this
        /// property uses cbData as its backing storage.
        /// </remarks>
        public uint cEnumColumnValue
        {
            get
            {
                return this.cbData;
            }

            set
            {
                this.cbData = value;
            }
        }

        /// <summary>
        /// Gets or sets an array of column values.
        /// This member is only used if <see cref="err"/> is not
        /// <see cref="JET_wrn.ColumnSingleValue"/>.
        /// </summary>
        /// <remarks>
        /// The unmanaged JET_ENUMCOLUMN structure is a union so this
        /// property uses pvData as its backing storage.
        /// </remarks>
        public NATIVE_ENUMCOLUMNVALUE* rgEnumColumnValue
        {
            get
            {
                return (NATIVE_ENUMCOLUMNVALUE*)this.pvData;
            }

            set
            {
                this.pvData = new IntPtr(value);
            }
        }
    }

    /// <summary>
    /// Enumerates the column values of a record using the JetEnumerateColumns
    /// function. JetEnumerateColumns returns an array of JET_ENUMCOLUMNVALUE
    /// structures. The array is returned in memory that was allocated using
    /// the callback that was supplied to that function.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_ENUMCOLUMN
    {
        /// <summary>
        /// Gets the columnid ID that was enumerated.
        /// </summary>
        public JET_COLUMNID columnid { get; internal set; }

        /// <summary>
        /// Gets the column status code that results from the enumeration.
        /// </summary>
        /// <seealso cref="JET_wrn.ColumnSingleValue"/>
        public JET_wrn err { get; internal set; }

        /// <summary>
        /// Gets the number of column values enumerated for the column.
        /// This member is only used if <see cref="err"/> is not
        /// <see cref="JET_wrn.ColumnSingleValue"/>.
        /// </summary>
        public int cEnumColumnValue { get; internal set; }

        /// <summary>
        /// Gets the enumerated column values for the column.
        /// This member is only used if <see cref="err"/> is not
        /// <see cref="JET_wrn.ColumnSingleValue"/>.
        /// </summary>
        public JET_ENUMCOLUMNVALUE[] rgEnumColumnValue { get; internal set; }

        /// <summary>
        /// Gets the size of the value that was enumerated for the column.
        /// This member is only used if <see cref="err"/> is equal to
        /// <see cref="JET_wrn.ColumnSingleValue"/>.
        /// </summary>
        public int cbData { get; internal set; }

        /// <summary>
        /// Gets the the value that was enumerated for the column.
        /// This member is only used if <see cref="err"/> is equal to
        /// <see cref="JET_wrn.ColumnSingleValue"/>.
        /// This points to memory allocated with the 
        /// <see cref="JET_PFNREALLOC"/> allocator callback passed to
        /// <see cref="Api.JetEnumerateColumns"/>. Remember to
        /// release the memory when finished.
        /// </summary>
        public IntPtr pvData { get; internal set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_ENUMCOLUMN"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_ENUMCOLUMN"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_ENUMCOLUMN(0x{0:x})", this.columnid);
        }

        /// <summary>
        /// Sets the fields of the object from a native JET_ENUMCOLUMN struct.
        /// </summary>
        /// <param name="value">
        /// The native enumcolumn to set the values from.
        /// </param>
        internal void SetFromNativeEnumColumn(NATIVE_ENUMCOLUMN value)
        {
            this.columnid = new JET_COLUMNID { Value = value.columnid };
            this.err = (JET_wrn)value.err;
            if (JET_wrn.ColumnSingleValue == this.err)
            {
                this.cbData = checked((int)value.cbData);
                this.pvData = value.pvData;
            }
            else
            {
                this.cEnumColumnValue = checked((int)value.cEnumColumnValue);
                this.rgEnumColumnValue = null;
            }
        }
    }
}