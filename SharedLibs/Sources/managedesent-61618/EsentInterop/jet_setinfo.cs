//-----------------------------------------------------------------------
// <copyright file="jet_setinfo.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_SETINFO structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_SETINFO
    {
        /// <summary>
        /// The size of a NATIVE_SETINFO structure.
        /// </summary>
        public static readonly int Size = Marshal.SizeOf(typeof(NATIVE_SETINFO));

        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Offset to the first byte to be set in a column of type JET_coltypLongBinary or JET_coltypLongText.
        /// </summary>
        public uint ibLongValue;
        
        /// <summary>
        /// The sequence number of value in a multi-valued column to be set.
        /// </summary>
        public uint itagSequence;
    }

    /// <summary>
    /// Settings for JetSetColumn.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    [Serializable]
    public class JET_SETINFO : IContentEquatable<JET_SETINFO>, IDeepCloneable<JET_SETINFO>
    {
        /// <summary>
        /// Offset to the first byte to be set in a column of type <see cref="JET_coltyp.LongBinary"/> or <see cref="JET_coltyp.LongText"/>.
        /// </summary>
        private int longValueOffset;

        /// <summary>
        /// The sequence number of value in a multi-valued column to be set.
        /// </summary>
        private int itag;

        /// <summary>
        /// Gets or sets offset to the first byte to be set in a column of type <see cref="JET_coltyp.LongBinary"/> or <see cref="JET_coltyp.LongText"/>.
        /// </summary>
        public int ibLongValue
        {
            get { return this.longValueOffset; }
            set { this.longValueOffset = value; }
        }

        /// <summary>
        /// Gets or sets the sequence number of value in a multi-valued column to be set. The array of values is one-based.
        /// The first value is sequence 1, not 0 (zero). If the record column has only one value then 1 should be passed
        /// as the itagSequence if that value is being replaced. A value of 0 (zero) means to add a new column value instance
        /// to the end of the sequence of column values.
        /// </summary>
        public int itagSequence
        {
            get { return this.itag; }
            set { this.itag = value; }
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_SETINFO"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_SETINFO"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_SETINFO(ibLongValue={0},itagSequence={1})",
                this.ibLongValue,
                this.itagSequence);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool ContentEquals(JET_SETINFO other)
        {
            if (null == other)
            {
                return false;
            }

            return this.ibLongValue == other.ibLongValue
                   && this.itagSequence == other.itagSequence;
        }

        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        public JET_SETINFO DeepClone()
        {
            return (JET_SETINFO)this.MemberwiseClone();
        }

        /// <summary>
        /// Gets the NATIVE_SETINFO structure that represents the object.
        /// </summary>
        /// <returns>A NATIVE_SETINFO structure whose fields match the class.</returns>
        internal NATIVE_SETINFO GetNativeSetinfo()
        {
            var setinfo = new NATIVE_SETINFO();
            setinfo.cbStruct = checked((uint)NATIVE_SETINFO.Size);
            setinfo.ibLongValue = checked((uint)this.ibLongValue);
            setinfo.itagSequence = checked((uint)this.itagSequence);
            return setinfo;
        }
    }
}