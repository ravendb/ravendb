//-----------------------------------------------------------------------
// <copyright file="jet_setcolumn.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;

    /// <summary>
    /// The native version of the <see cref="JET_SETCOLUMN"/> structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.DocumentationRules",
        "SA1600:ElementsMustBeDocumented",
        Justification = "Internal interop struct only.")]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_SETCOLUMN
    {
        public uint columnid;
        public IntPtr pvData;
        public uint cbData;
        public uint grbit;
        public uint ibLongValue;
        public uint itagSequence;
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
    public class JET_SETCOLUMN
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
        public JET_err err { get; internal set; }

        /// <summary>
        /// Check to see if cbData is negative or greater than cbData.
        /// </summary>
        internal void Validate()
        {
            if (this.cbData < 0)
            {
                throw new ArgumentOutOfRangeException("cbData", "data length cannot be negative");    
            }

            if ((null == this.pvData && 0 != this.cbData) || (null != this.pvData && this.cbData > this.pvData.Length))
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
                cbData = checked((uint) this.cbData),
                grbit = (uint) this.grbit,
                ibLongValue = checked((uint) this.ibLongValue),
                itagSequence = checked((uint) this.itagSequence),
            };
            return setinfo;
        }
    }
}