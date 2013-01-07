//-----------------------------------------------------------------------
// <copyright file="jet_indexrange.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_INDEXRANGE structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_INDEXRANGE
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Cursor containing the index range.
        /// </summary>
        public IntPtr tableid;

        /// <summary>
        /// Index range options.
        /// </summary>
        public uint grbit;

        /// <summary>
        /// Create a NATIVE_INDEXRANGE from a cursor.
        /// </summary>
        /// <param name="tableid">The cursor containing the index range.</param>
        /// <returns>A new NATIVE_INDEXRANGE on the cursor.</returns>
        public static NATIVE_INDEXRANGE MakeIndexRangeFromTableid(JET_TABLEID tableid)
        {
            var s = new NATIVE_INDEXRANGE
            {
                tableid = tableid.Value,
                grbit = (uint)IndexRangeGrbit.RecordInIndex,
            };
            s.cbStruct = (uint)Marshal.SizeOf(s);
            return s;
        }
    }

    /// <summary>
    /// Identifies an index range when it is used with the JetIntersectIndexes function.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_INDEXRANGE : IContentEquatable<JET_INDEXRANGE>, IDeepCloneable<JET_INDEXRANGE>
    {
        /// <summary>
        /// Initializes a new instance of the JET_INDEXRANGE class.
        /// </summary>
        public JET_INDEXRANGE()
        {
            // set the grbit to the only valid value
            this.grbit = IndexRangeGrbit.RecordInIndex;
        }

        /// <summary>
        /// Gets or sets the cursor containing the index range. The cursor should have an
        /// index range set with JetSetIndexRange.
        /// </summary>
        public JET_TABLEID tableid { get; set; }

        /// <summary>
        /// Gets or sets the indexrange option.
        /// </summary>
        public IndexRangeGrbit grbit { get; set; }

        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        public JET_INDEXRANGE DeepClone()
        {
            return (JET_INDEXRANGE)this.MemberwiseClone();
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_INDEXRANGE"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_INDEXRANGE"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_INDEXRANGE(0x{0:x},{1})", this.tableid.Value, this.grbit);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool ContentEquals(JET_INDEXRANGE other)
        {
            if (null == other)
            {
                return false;
            }

            return this.tableid == other.tableid
                   && this.grbit == other.grbit;
        }

        /// <summary>
        /// Get a NATIVE_INDEXRANGE structure representing the object.
        /// </summary>
        /// <returns>A NATIVE_INDEXRANGE whose members match the class.</returns>
        internal NATIVE_INDEXRANGE GetNativeIndexRange()
        {
            var indexrange = new NATIVE_INDEXRANGE();
            indexrange.cbStruct = (uint)Marshal.SizeOf(indexrange);
            indexrange.tableid = this.tableid.Value;
            indexrange.grbit = (uint)this.grbit;
            return indexrange;
        }
    }
}

