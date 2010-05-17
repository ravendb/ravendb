//-----------------------------------------------------------------------
// <copyright file="jet_recsize.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;

    /// <summary>
    /// Used by <see cref="Api.JetGetRecordSize"/> to return information about a record's usage
    /// requirements in user data space, number of set columns, number of
    /// values, and ESENT record structure overhead space.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public struct JET_RECSIZE
    {
        /// <summary>
        /// Gets the user data set in the record.
        /// </summary>
        public long cbData { get; internal set; }

        /// <summary>
        /// Gets the user data set in the record, but stored in the long-value tree.
        /// </summary>
        public long cbLongValueData { get; internal set; }

        /// <summary>
        /// Gets the overhead of the ESENT record structure for this record.
        /// This includes the record's key size.
        /// </summary>
        public long cbOverhead { get; internal set; }

        /// <summary>
        /// Gets the overhead of the long-value data.
        /// </summary>
        public long cbLongValueOverhead { get; internal set; }

        /// <summary>
        /// Gets the total number of fixed and variable columns set in this record.
        /// </summary>
        public long cNonTaggedColumns { get; internal set; }

        /// <summary>
        /// Gets the total number of tagged columns set in this record.
        /// </summary>
        public long cTaggedColumns { get; internal set; }

        /// <summary>
        /// Gets the total number of long values stored in the long-value tree
        /// for this record. This does not include intrinsic long values.
        /// </summary>
        public long cLongValues { get; internal set; }

        /// <summary>
        /// Gets the accumulation of the total number of values beyond the first
        /// for all columns in the record.
        /// </summary>
        public long cMultiValues { get; internal set; }

        /// <summary>
        /// Gets the total number of columns in the record which are compressed.
        /// </summary>
        public long cCompressedColumns { get; internal set; }

        /// <summary>
        /// Gets the compressed size of user data in record. This is the same
        /// as <see cref="cbData"/> if no intrinsic long-values are compressed).
        /// </summary>
        public long cbDataCompressed { get; internal set; }

        /// <summary>
        /// Gets the compressed size of user data in the long-value tree. This is
        /// the same as <see cref="cbLongValueData"/> if no separated long values
        /// are compressed.
        /// </summary>
        public long cbLongValueDataCompressed { get; internal set; }

        /// <summary>
        /// Add the sizes in two JET_RECSIZE structures.
        /// </summary>
        /// <param name="s1">The first JET_RECSIZE.</param>
        /// <param name="s2">The second JET_RECSIZE.</param>
        /// <returns>A JET_RECSIZE containing the result of adding the sizes in s1 and s2.</returns>
        public static JET_RECSIZE operator +(JET_RECSIZE s1, JET_RECSIZE s2)
        {
            checked
            {
                return new JET_RECSIZE
                {
                    cbData = s1.cbData + s2.cbData,
                    cbDataCompressed = s1.cbDataCompressed + s2.cbDataCompressed,
                    cbLongValueData = s1.cbLongValueData + s2.cbLongValueData,
                    cbLongValueDataCompressed = s1.cbLongValueDataCompressed + s2.cbLongValueDataCompressed,
                    cbLongValueOverhead = s1.cbLongValueOverhead + s2.cbLongValueOverhead,
                    cbOverhead = s1.cbOverhead + s2.cbOverhead,
                    cCompressedColumns = s1.cCompressedColumns + s2.cCompressedColumns,
                    cLongValues = s1.cLongValues + s2.cLongValues,
                    cMultiValues = s1.cMultiValues + s2.cMultiValues,
                    cNonTaggedColumns = s1.cNonTaggedColumns + s2.cNonTaggedColumns,
                    cTaggedColumns = s1.cTaggedColumns + s2.cTaggedColumns,                    
                };
            }
        }

        /// <summary>
        /// Calculate the difference in sizes between two JET_RECSIZE structures.
        /// </summary>
        /// <param name="s1">The first JET_RECSIZE.</param>
        /// <param name="s2">The second JET_RECSIZE.</param>
        /// <returns>A JET_RECSIZE containing the difference in sizes between s1 and s2.</returns>
        public static JET_RECSIZE operator -(JET_RECSIZE s1, JET_RECSIZE s2)
        {
            checked
            {
                return new JET_RECSIZE
                {
                    cbData = s1.cbData - s2.cbData,
                    cbDataCompressed = s1.cbDataCompressed - s2.cbDataCompressed,
                    cbLongValueData = s1.cbLongValueData - s2.cbLongValueData,
                    cbLongValueDataCompressed = s1.cbLongValueDataCompressed - s2.cbLongValueDataCompressed,
                    cbLongValueOverhead = s1.cbLongValueOverhead - s2.cbLongValueOverhead,
                    cbOverhead = s1.cbOverhead - s2.cbOverhead,
                    cCompressedColumns = s1.cCompressedColumns - s2.cCompressedColumns,
                    cLongValues = s1.cLongValues - s2.cLongValues,
                    cMultiValues = s1.cMultiValues - s2.cMultiValues,
                    cNonTaggedColumns = s1.cNonTaggedColumns - s2.cNonTaggedColumns,
                    cTaggedColumns = s1.cTaggedColumns - s2.cTaggedColumns,
                };
            }
        }

        /// <summary>
        /// Sets the fields of the object from a NATIVE_RECSIZE struct.
        /// </summary>
        /// <param name="value">
        /// The native recsize to set the values from.
        /// </param>
        internal void SetFromNativeRecsize(NATIVE_RECSIZE value)
        {
            // This is used on versions of ESENT that don't support JetGetRecordSize2.
            // That means compression isn't supported so we can default the compression
            // members to 'non compressed' values.
            checked
            {
                this.cbData = (long)value.cbData;
                this.cbDataCompressed = (long)value.cbData;
                this.cbLongValueData = (long)value.cbLongValueData;
                this.cbLongValueDataCompressed = (long)value.cbLongValueData;
                this.cbLongValueOverhead = (long)value.cbLongValueOverhead;
                this.cbOverhead = (long)value.cbOverhead;
                this.cCompressedColumns = 0;
                this.cLongValues = (long)value.cLongValues;
                this.cMultiValues = (long)value.cMultiValues;
                this.cNonTaggedColumns = (long)value.cNonTaggedColumns;
                this.cTaggedColumns = (long)value.cTaggedColumns;
            }
        }

        /// <summary>
        /// Sets the fields of the object from a NATIVE_RECSIZE2 struct.
        /// </summary>
        /// <param name="value">
        /// The native recsize to set the values from.
        /// </param>
        internal void SetFromNativeRecsize(NATIVE_RECSIZE2 value)
        {
            checked
            {
                this.cbData = (long)value.cbData;
                this.cbDataCompressed = (long)value.cbDataCompressed;
                this.cbLongValueData = (long)value.cbLongValueData;
                this.cbLongValueDataCompressed = (long)value.cbLongValueDataCompressed;
                this.cbLongValueOverhead = (long)value.cbLongValueOverhead;
                this.cbOverhead = (long)value.cbOverhead;
                this.cCompressedColumns = (long)value.cCompressedColumns;
                this.cLongValues = (long)value.cLongValues;
                this.cMultiValues = (long)value.cMultiValues;
                this.cNonTaggedColumns = (long)value.cNonTaggedColumns;
                this.cTaggedColumns = (long)value.cTaggedColumns;
            }
        }

        /// <summary>
        /// Gets a NATIVE_RECSIZE containing the values in this object.
        /// </summary>
        /// <returns>
        /// A NATIVE_RECSIZE initialized with the values in the object.
        /// </returns>
        internal NATIVE_RECSIZE GetNativeRecsize()
        {
            unchecked
            {
                return new NATIVE_RECSIZE
                {
                    cbData = (ulong)this.cbData,
                    cbLongValueData = (ulong)this.cbLongValueData,
                    cbLongValueOverhead = (ulong)this.cbLongValueOverhead,
                    cbOverhead = (ulong)this.cbOverhead,
                    cLongValues = (ulong)this.cLongValues,
                    cMultiValues = (ulong)this.cMultiValues,
                    cNonTaggedColumns = (ulong)this.cNonTaggedColumns,
                    cTaggedColumns = (ulong)this.cTaggedColumns,
                };
            }
        }

        /// <summary>
        /// Gets a NATIVE_RECSIZE2 containing the values in this object.
        /// </summary>
        /// <returns>
        /// A NATIVE_RECSIZE2 initialized with the values in the object.
        /// </returns>
        internal NATIVE_RECSIZE2 GetNativeRecsize2()
        {
            unchecked
            {
                return new NATIVE_RECSIZE2
                {
                    cbData = (ulong)this.cbData,
                    cbDataCompressed = (ulong)this.cbDataCompressed,
                    cbLongValueData = (ulong)this.cbLongValueData,
                    cbLongValueDataCompressed = (ulong)this.cbLongValueDataCompressed,
                    cbLongValueOverhead = (ulong)this.cbLongValueOverhead,
                    cbOverhead = (ulong)this.cbOverhead,
                    cCompressedColumns = (ulong)this.cCompressedColumns,
                    cLongValues = (ulong)this.cLongValues,
                    cMultiValues = (ulong)this.cMultiValues,
                    cNonTaggedColumns = (ulong)this.cNonTaggedColumns,
                    cTaggedColumns = (ulong)this.cTaggedColumns,
                };
            }
        }
    }

    /// <summary>
    /// The native version of the JET_RECSIZE structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_RECSIZE
    {
        /// <summary>
        /// User data in record.
        /// </summary>
        public ulong cbData;

        /// <summary>
        /// User data associated with the record but stored in the long-value
        /// tree. Does NOT count intrinsic long-values.
        /// </summary>
        public ulong cbLongValueData;

        /// <summary>
        /// Record overhead.
        /// </summary>
        public ulong cbOverhead;

        /// <summary>
        /// Overhead of long-value data. Does not count intrinsic long-values.
        /// </summary>
        public ulong cbLongValueOverhead;

        /// <summary>
        /// Total number of fixed/variable columns.
        /// </summary>
        public ulong cNonTaggedColumns;

        /// <summary>
        /// Total number of tagged columns.
        /// </summary>
        public ulong cTaggedColumns;

        /// <summary>
        /// Total number of values stored in the long-value tree for this record.
        /// Does NOT count intrinsic long-values.
        /// </summary>
        public ulong cLongValues;

        /// <summary>
        /// Total number of values beyond the first for each column in the record.
        /// </summary>
        public ulong cMultiValues;
    }

    /// <summary>
    /// The native version of the JET_RECSIZE2 structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_RECSIZE2
    {
        /// <summary>
        /// User data in record.
        /// </summary>
        public ulong cbData;

        /// <summary>
        /// User data associated with the record but stored in the long-value
        /// tree. Does NOT count intrinsic long-values.
        /// </summary>
        public ulong cbLongValueData;

        /// <summary>
        /// Record overhead.
        /// </summary>
        public ulong cbOverhead;

        /// <summary>
        /// Overhead of long-value data. Does not count intrinsic long-values.
        /// </summary>
        public ulong cbLongValueOverhead;

        /// <summary>
        /// Total number of fixed/variable columns.
        /// </summary>
        public ulong cNonTaggedColumns;

        /// <summary>
        /// Total number of tagged columns.
        /// </summary>
        public ulong cTaggedColumns;

        /// <summary>
        /// Total number of values stored in the long-value tree for this record.
        /// Does NOT count intrinsic long-values.
        /// </summary>
        public ulong cLongValues;

        /// <summary>
        /// Total number of values beyond the first for each column in the record.
        /// </summary>
        public ulong cMultiValues;

        /// <summary>
        /// Total number of columns which are compressed.
        /// </summary>
        public ulong cCompressedColumns;

        /// <summary>
        /// Compressed size of user data in record. Same as cbData if no intrinsic
        /// long-values are compressed.
        /// </summary>
        public ulong cbDataCompressed;

        /// <summary>
        /// Compressed size of user data in the long-value tree. Same as
        /// cbLongValue data if no separated long values are compressed.
        /// </summary>
        public ulong cbLongValueDataCompressed;
    }
}