//-----------------------------------------------------------------------
// <copyright file="jet_retinfo.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_RETINFO structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_RETINFO
    {
        /// <summary>
        /// The size of a NATIVE_RETINFO structure.
        /// </summary>
        public static readonly int Size = Marshal.SizeOf(typeof(NATIVE_RETINFO));

        /// <summary>
        /// Size of this structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Offset of the long value to retrieve.
        /// </summary>
        public uint ibLongValue;

        /// <summary>
        /// Itag sequence to retrieve.
        /// </summary>
        public uint itagSequence;

        /// <summary>
        /// Returns the columnid of the next tagged column.
        /// </summary>
        public uint columnidNextTagged;
    }

    /// <summary>
    /// Contains optional input and output parameters for JetRetrieveColumn.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_RETINFO : IContentEquatable<JET_RETINFO>, IDeepCloneable<JET_RETINFO>
    {
        /// <summary>
        /// Gets or sets the offset to the first byte to be retrieved from a column of
        /// type <see cref="JET_coltyp.LongBinary"/>, or <see cref="JET_coltyp.LongText"/>.
        /// </summary>
        public int ibLongValue { get; set; }

        /// <summary>
        /// Gets or sets the sequence number of value in a multi-valued column.
        /// The array of values is one-based. The first value is
        /// sequence 1, not 0. If the record column has only one value then
        /// 1 should be passed as the itagSequence.
        /// </summary>
        public int itagSequence { get; set; }

        /// <summary>
        /// Gets the columnid of the retrieved tagged, multi-valued or
        /// sparse, column when all tagged columns are retrieved by passing
        /// 0 as the columnid to JetRetrieveColumn.
        /// </summary>
        public JET_COLUMNID columnidNextTagged { get; internal set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_RETINFO"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_RETINFO"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_RETINFO(ibLongValue={0},itagSequence={1})",
                this.ibLongValue,
                this.itagSequence);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool ContentEquals(JET_RETINFO other)
        {
            if (null == other)
            {
                return false;
            }

            return this.ibLongValue == other.ibLongValue
                   && this.itagSequence == other.itagSequence
                   && this.columnidNextTagged == other.columnidNextTagged;
        }

        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        public JET_RETINFO DeepClone()
        {
            return (JET_RETINFO)this.MemberwiseClone();
        }

        /// <summary>
        /// Get a NATIVE_RETINFO structure representing the object.
        /// </summary>
        /// <returns>A NATIVE_RETINFO whose members match the class.</returns>
        internal NATIVE_RETINFO GetNativeRetinfo()
        {
            var retinfo = new NATIVE_RETINFO();
            retinfo.cbStruct = checked((uint)NATIVE_RETINFO.Size);
            retinfo.ibLongValue = checked((uint)this.ibLongValue);
            retinfo.itagSequence = checked((uint)this.itagSequence);
            return retinfo;
        }

        /// <summary>
        /// Sets the fields of the object from a NATIVE_RETINFO structure.
        /// </summary>
        /// <param name="value">The NATIVE_RETINFO which will be used to set the fields.</param>
        internal void SetFromNativeRetinfo(NATIVE_RETINFO value)
        {
            this.ibLongValue = checked((int)value.ibLongValue);
            this.itagSequence = checked((int)value.itagSequence);

            var columnid = new JET_COLUMNID { Value = value.columnidNextTagged };
            this.columnidNextTagged = columnid;
        }
    }
}