//-----------------------------------------------------------------------
// <copyright file="jet_objectinfo.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_OBJECTINFO structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_OBJECTINFO
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Holds the JET_OBJTYP of the structure. Currently only tables will be
        /// returned (that is, <see cref="JET_objtyp.Table"/>).
        /// </summary>
        public uint objtyp;

        /// <summary>
        /// Obsolete. Do not use.
        /// </summary>
        [Obsolete("Unused member")]
        public double ignored1;

        /// <summary>
        /// Obsolete. Do not use.
        /// </summary>
        [Obsolete("Unused member")]
        public double ignored2;

        /// <summary>
        /// A group of bits that contain table options.
        /// </summary>
        public uint grbit;

        /// <summary>
        /// Table type flags.
        /// </summary>
        public uint flags;

        /// <summary>
        /// Number of records in the table.
        /// </summary>
        public uint cRecord;

        /// <summary>
        /// Number of pages used by the table.
        /// </summary>
        public uint cPage;
    }

    /// <summary>
    /// The JET_OBJECTINFO structure holds information about an object.
    /// Tables are the only object types that are currently supported.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_OBJECTINFO
    {
        /// <summary>
        /// Gets the JET_OBJTYP of the table. Currently only tables will be
        /// returned (that is, <see cref="JET_objtyp.Table"/>).
        /// </summary>
        public JET_objtyp objtyp { get; private set; }

        /// <summary>
        /// Gets the table options.
        /// </summary>
        public ObjectInfoGrbit grbit { get; private set; }

        /// <summary>
        /// Gets the table type flags.
        /// </summary>
        public ObjectInfoFlags flags { get; private set; }

        /// <summary>
        /// Gets the number of records in the table.
        /// </summary>
        public int cRecord { get; private set; }

        /// <summary>
        /// Gets the number of pages used by the table.
        /// </summary>
        public int cPage { get; private set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_OBJECTINFO"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_OBJECTINFO"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_OBJECTINFO({0})", this.flags);
        }

        /// <summary>
        /// Sets the fields of the object from a native JET_OBJECTINFO struct.
        /// </summary>
        /// <param name="value">
        /// The native objectlist to set the values from.
        /// </param>
        internal void SetFromNativeObjectinfo(ref NATIVE_OBJECTINFO value)
        {
            unchecked
            {
                this.objtyp = (JET_objtyp)value.objtyp;
                this.grbit = (ObjectInfoGrbit)value.grbit;
                this.flags = (ObjectInfoFlags)value.flags;
                this.cRecord = (int)value.cRecord;
                this.cPage = (int)value.cPage;
            }
        }
    }
}