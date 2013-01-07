//-----------------------------------------------------------------------
// <copyright file="jet_setcolumn.cs" company="Microsoft Corporation">
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
    /// The native version of the <see cref="JET_SETCOLUMN"/> structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_SETCOLUMN
    {
        /// <summary>
        /// Columnid to set.
        /// </summary>
        public uint columnid;

        /// <summary>
        /// Data to set.
        /// </summary>
        public IntPtr pvData;

        /// <summary>
        /// Size of data to set.
        /// </summary>
        public uint cbData;

        /// <summary>
        /// SetColumns options.
        /// </summary>
        public uint grbit;

        /// <summary>
        /// Long-value offset to set.
        /// </summary>
        public uint ibLongValue;

        /// <summary>
        /// Itag sequence to set.
        /// </summary>
        public uint itagSequence;

        /// <summary>
        /// Returns the error from setting the column.
        /// </summary>
        public uint err;
    }

    /// <summary>
    /// Contains input and output parameters for <see cref="Api.JetSetColumns"/>.
    /// Fields in the structure describe what column value to set, how to set it,
    /// and where to get the column set data.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_SETCOLUMN : IContentEquatable<JET_SETCOLUMN>, IDeepCloneable<JET_SETCOLUMN>
    {
        /// <summary>
        /// Gets or sets the column identifier for a column to set.
        /// </summary>
        public JET_COLUMNID columnid { get; set; }

        /// <summary>
        /// Gets or sets a pointer to the data to set.
        /// </summary>
        public byte[] pvData { get; set; }

        /// <summary>
        /// Gets or sets the offset of the data to set.
        /// </summary>
        public int ibData { get; set; }

        /// <summary>
        /// Gets or sets the size of the data to set.
        /// </summary>
        public int cbData { get; set; }

        /// <summary>
        /// Gets or sets options for the set column operation.
        /// </summary>
        public SetColumnGrbit grbit { get; set; }

        /// <summary>
        /// Gets or sets offset to the first byte to be set in a column of type
        /// <see cref="JET_coltyp.LongBinary"/> or <see cref="JET_coltyp.LongText"/>.
        /// </summary>
        public int ibLongValue { get; set; }

        /// <summary>
        /// Gets or sets the sequence number of value in a multi-valued column to be set. The array of values is one-based.
        /// The first value is sequence 1, not 0 (zero). If the record column has only one value then 1 should be passed
        /// as the itagSequence if that value is being replaced. A value of 0 (zero) means to add a new column value instance
        /// to the end of the sequence of column values.
        /// </summary>
        public int itagSequence { get; set; }

        /// <summary>
        /// Gets the error code or warning returned from the set column operation.
        /// </summary>
        public JET_wrn err { get; internal set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_SETCOLUMN"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_SETCOLUMN"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_SETCOLUMN(0x{0:x},{1},ibLongValue={2},itagSequence={3})",
                this.columnid.Value,
                Util.DumpBytes(this.pvData, this.ibData, this.cbData),
                this.ibLongValue,
                this.itagSequence);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool ContentEquals(JET_SETCOLUMN other)
        {
            if (null == other)
            {
                return false;
            }

            this.CheckDataSize();
            other.CheckDataSize();
            return this.columnid == other.columnid
                   && this.ibData == other.ibData
                   && this.cbData == other.cbData
                   && this.grbit == other.grbit
                   && this.ibLongValue == other.ibLongValue
                   && this.itagSequence == other.itagSequence
                   && this.err == other.err
                   && Util.ArrayEqual(this.pvData, other.pvData, this.ibData, this.cbData);
        }

        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        public JET_SETCOLUMN DeepClone()
        {
            JET_SETCOLUMN result = (JET_SETCOLUMN)this.MemberwiseClone();
            if (null != this.pvData)
            {
                result.pvData = new byte[this.pvData.Length];
                Array.Copy(this.pvData, result.pvData, this.cbData);
            }

            return result;
        }

        /// <summary>
        /// Check to see if cbData is negative or greater than the length of pvData.
        /// Check to see if ibData is negative or greater than the length of pvData.
        /// </summary>
        internal void CheckDataSize()
        {
            if (this.cbData < 0)
            {
                throw new ArgumentOutOfRangeException("cbData", "data length cannot be negative");    
            }

            if (this.ibData < 0)
            {
                throw new ArgumentOutOfRangeException("ibData", "data offset cannot be negative");
            }

            if (0 != this.ibData && (null == this.pvData || this.ibData >= this.pvData.Length))
            {
                throw new ArgumentOutOfRangeException(
                    "ibData",
                    this.ibData,
                    "cannot be greater than the length of the pvData");
            }

            if ((null == this.pvData && 0 != this.cbData) || (null != this.pvData && this.cbData > (this.pvData.Length - this.ibData)))
            {
                throw new ArgumentOutOfRangeException(
                    "cbData",
                    this.cbData,
                    "cannot be greater than the length of the pvData");
            }
            
            if (this.itagSequence < 0)
            {
                throw new ArgumentOutOfRangeException("itagSequence", this.itagSequence, "cannot be negative");
            }

            if (this.ibLongValue < 0)
            {
                throw new ArgumentOutOfRangeException("ibLongValue", this.ibLongValue, "cannot be negative");
            }
        }

        /// <summary>
        /// Gets the NATIVE_SETCOLUMN structure that represents the object.
        /// </summary>
        /// <returns>A NATIVE_SETCOLUMN structure whose fields match the class.</returns>
        internal NATIVE_SETCOLUMN GetNativeSetcolumn()
        {
            var setinfo = new NATIVE_SETCOLUMN
            {
                columnid = this.columnid.Value,
                cbData = checked((uint)this.cbData),
                grbit = (uint)this.grbit,
                ibLongValue = checked((uint)this.ibLongValue),
                itagSequence = checked((uint)this.itagSequence),
            };
            return setinfo;
        }
    }
}