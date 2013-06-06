//-----------------------------------------------------------------------
// <copyright file="jet_enumcolumnid.cs" company="Microsoft Corporation">
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
    /// The native (unmanaged) version of the
    /// <see cref="JET_ENUMCOLUMNID"/> class.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal unsafe struct NATIVE_ENUMCOLUMNID
    {
        /// <summary>
        /// Column ID to enumerate.
        /// </summary>
        public uint columnid;

        /// <summary>
        /// Count of column values to enumerate.
        /// </summary>
        public uint ctagSequence;

        /// <summary>
        /// Column values to enumerate.
        /// </summary>
        public uint* rgtagSequence;
    }

    /// <summary>
    /// Enumerates a specific set of columns and, optionally, a specific set
    /// of multiple values for those columns when the JetEnumerateColumns
    /// function is used. JetEnumerateColumns optionally takes an array of
    /// JET_ENUMCOLUMNID structures.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_ENUMCOLUMNID
    {
        /// <summary>
        /// Gets or sets the columnid ID to enumerate.
        /// </summary>
        /// <remarks>
        /// If the column ID is 0 (zero) then the enumeration of this column is
        /// skipped and a corresponding slot in the output array of JET_ENUMCOLUMN
        /// structures will be generated with a column state of JET_wrnColumnSkipped.
        /// </remarks>
        public JET_COLUMNID columnid { get; set; }

        /// <summary>
        /// Gets or sets the count of column values (by one-based index) to
        /// enumerate for the specified column ID. If ctagSequence is 0 (zero) then
        /// rgtagSequence is ignored and all column values for the specified column
        /// ID will be enumerated.
        /// </summary>
        public int ctagSequence { get; set; }

        /// <summary>
        /// Gets or sets the array of one-based indices into the array of column values for a
        /// given column. A single element is an itagSequence which is defined in
        /// JET_RETRIEVECOLUMN. An itagSequence of 0 (zero) means "skip". An
        /// itagSequence of 1 means return the first column value of the column,
        /// 2 means the second, and so on.
        /// </summary>
        public int[] rgtagSequence { get; set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_ENUMCOLUMNID"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_ENUMCOLUMNID"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_ENUMCOLUMNID(0x{0:x})", this.columnid);
        }

        /// <summary>
        /// Check to see if ctagSequence is negative or greater than the length
        /// of rgtagSequence.
        /// </summary>
        internal void CheckDataSize()
        {
            if (this.ctagSequence < 0)
            {
                throw new ArgumentOutOfRangeException("ctagSequence", "ctagSequence cannot be negative");
            }

            if ((null == this.rgtagSequence && 0 != this.ctagSequence) || (null != this.rgtagSequence && this.ctagSequence > this.rgtagSequence.Length))
            {
                throw new ArgumentOutOfRangeException(
                    "ctagSequence",
                    this.ctagSequence,
                    "cannot be greater than the length of the pvData");
            }
        }

        /// <summary>
        /// Gets the native (interop) version of this object.
        /// </summary>
        /// <returns>A NATIVE_ENUMCOLUMNID representing this object.</returns>
        internal NATIVE_ENUMCOLUMNID GetNativeEnumColumnid()
        {
            this.CheckDataSize();          
            var value = new NATIVE_ENUMCOLUMNID
            {
                columnid = this.columnid.Value,
                ctagSequence = checked((uint)this.ctagSequence)
            };
            return value;
        }
    }
}