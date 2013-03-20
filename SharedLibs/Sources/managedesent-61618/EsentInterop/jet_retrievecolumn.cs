//-----------------------------------------------------------------------
// <copyright file="jet_retrievecolumn.cs" company="Microsoft Corporation">
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
    /// The native version of the <see cref="JET_RETRIEVECOLUMN"/> structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_RETRIEVECOLUMN
    {
        /// <summary>
        /// The column identifier for the column to retrieve.
        /// </summary>
        public uint columnid;

        /// <summary>
        /// A pointer to begin storing data that is retrieved from the
        /// column value.
        /// </summary>
        public IntPtr pvData;

        /// <summary>
        /// The size of allocation beginning at pvData, in bytes. The
        /// retrieve column operation will not store more data at pvData
        /// than cbData.
        /// </summary>
        public uint cbData;

        /// <summary>
        /// The size, in bytes, of data that is retrieved by a retrieve
        /// column operation.
        /// </summary>
        public uint cbActual;

        /// <summary>
        /// A group of bits that contain the options for column retrieval.
        /// </summary>
        public uint grbit;

        /// <summary>
        /// The offset to the first byte to be retrieved from a column of
        /// type <see cref="JET_coltyp.LongBinary"/> or
        /// <see cref="JET_coltyp.LongText"/>.
        /// </summary>
        public uint ibLongValue;

        /// <summary>
        /// The sequence number of the values that are contained in a
        /// multi-valued column. If the itagSequence is 0 then the number
        /// of instances of a multi-valued column are returned instead of
        /// any column data. 
        /// </summary>
        public uint itagSequence;

        /// <summary>
        /// The columnid of the tagged, multi-valued, or sparse column
        /// when all tagged columns are retrieved by passing 0 as the
        /// columnid.
        /// </summary>
        public uint columnidNextTagged;

        /// <summary>
        /// Error codes and warnings returned from the retrieval of the column.
        /// </summary>
        public int err;
    }

    /// <summary>
    /// Contains input and output parameters for <see cref="Api.JetRetrieveColumns"/>.
    /// Fields in the structure describe what column value to retrieve, how to
    /// retrieve it, and where to save results.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_RETRIEVECOLUMN
    {
        /// <summary>
        /// Gets or sets the column identifier for the column to retrieve.
        /// </summary>
        public JET_COLUMNID columnid { get; set; }

        /// <summary>
        /// Gets or sets the buffer that will store data that is retrieved from the
        /// column.
        /// </summary>
        public byte[] pvData { get; set; }

        /// <summary>
        /// Gets or sets the offset in the buffer that data will be stored in.
        /// </summary>
        public int ibData { get; set; }

        /// <summary>
        /// Gets or sets the size of the <see cref="pvData"/> buffer, in bytes. The
        /// retrieve column operation will not store more data in pvData
        /// than cbData.
        /// </summary>
        public int cbData { get; set; }

        /// <summary>
        /// Gets the size, in bytes, of data that is retrieved by a retrieve
        /// column operation.
        /// </summary>
        public int cbActual { get; private set; }

        /// <summary>
        /// Gets or sets the options for column retrieval.
        /// </summary>
        public RetrieveColumnGrbit grbit { get; set; }

        /// <summary>
        /// Gets or sets the offset to the first byte to be retrieved from a column of
        /// type <see cref="JET_coltyp.LongBinary"/> or
        /// <see cref="JET_coltyp.LongText"/>.
        /// </summary>
        public int ibLongValue { get; set; }

        /// <summary>
        /// Gets or sets the sequence number of the values that are contained in a
        /// multi-valued column. If the itagSequence is 0 then the number
        /// of instances of a multi-valued column are returned instead of
        /// any column data. 
        /// </summary>
        public int itagSequence { get; set; }

        /// <summary>
        /// Gets the columnid of the tagged, multi-valued, or sparse column
        /// when all tagged columns are retrieved by passing 0 as the
        /// columnid.
        /// </summary>
        public JET_COLUMNID columnidNextTagged { get; private set; }

        /// <summary>
        /// Gets the warning returned from the retrieval of the column.
        /// </summary>
        public JET_wrn err { get; private set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_RETRIEVECOLUMN"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_RETRIEVECOLUMN"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_RETRIEVECOLUMN(0x{0:x})", this.columnid);
        }

        /// <summary>
        /// Check to see if cbData is negative or greater than cbData.
        /// </summary>
        internal void CheckDataSize()
        {
            if (this.cbData < 0)
            {
                throw new ArgumentOutOfRangeException("cbData", this.cbData, "data length cannot be negative");
            }

            if (this.ibData < 0)
            {
                throw new ArgumentOutOfRangeException("ibData", this.cbData, "data offset cannot be negative");
            }

            if (0 != this.ibData && (null == this.pvData || this.ibData >= this.pvData.Length))
            {
                throw new ArgumentOutOfRangeException(
                    "ibData",
                    this.ibData,
                    "cannot be greater than the length of the pvData buffer");
            }

            if ((null == this.pvData && 0 != this.cbData) || (null != this.pvData && this.cbData > (this.pvData.Length - this.ibData)))
            {
                throw new ArgumentOutOfRangeException(
                    "cbData",
                    this.cbData,
                    "cannot be greater than the length of the pvData buffer");
            }
        }

        /// <summary>
        /// Gets the NATIVE_RETRIEVECOLUMN structure that represents the object.
        /// </summary>
        /// <param name="retrievecolumn">The NATIVE_RETRIEVECOLUMN structure to fill in.</param>
        /// <remarks>
        /// This takes a reference because a NATIVE_RETRIEVECOLUMN is quite large (40 bytes)
        /// so copying it around can be expensive.
        /// </remarks>
        internal void GetNativeRetrievecolumn(ref NATIVE_RETRIEVECOLUMN retrievecolumn)
        {
            retrievecolumn.columnid = this.columnid.Value;
            retrievecolumn.cbData = unchecked((uint)this.cbData); // guaranteed to not be negative
            retrievecolumn.grbit = (uint)this.grbit;
            retrievecolumn.ibLongValue = checked((uint)this.ibLongValue);
            retrievecolumn.itagSequence = checked((uint)this.itagSequence);
        }

        /// <summary>
        /// Update the output members of the class from a NATIVE_RETRIEVECOLUMN
        /// structure. This should be done after the columns are retrieved.
        /// </summary>
        /// <param name="native">
        /// The structure containing the updated output fields.
        /// </param>
        /// <remarks>
        /// This takes a reference because a NATIVE_RETRIEVECOLUMN is quite large (40 bytes)
        /// so copying it around can be expensive.
        /// </remarks>
        internal void UpdateFromNativeRetrievecolumn(ref NATIVE_RETRIEVECOLUMN native)
        {
            this.cbActual = checked((int)native.cbActual);
            this.columnidNextTagged = new JET_COLUMNID { Value = native.columnidNextTagged };
            this.itagSequence = checked((int)native.itagSequence);
            this.err = (JET_wrn)native.err;
        }
    }
}