//-----------------------------------------------------------------------
// <copyright file="jet_conditionalcolumn.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;

    /// <summary>
    /// The native version of the JET_CONDITIONALCOLUMN structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_CONDITIONALCOLUMN
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Name of the column.
        /// </summary>
        public string szColumnName;

        /// <summary>
        /// Conditional column option.
        /// </summary>
        public uint grbit;
    }

    /// <summary>
    /// Defines how conditional indexing is performed for a given index. A
    /// conditional index contains an index entry for only those rows that
    /// match the specified condition. However, the conditional column is not
    /// part of the index's key, it only controls the presence of the index entry.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_CONDITIONALCOLUMN
    {
        /// <summary>
        /// Gets or sets the name of the conditional column.
        /// </summary>
        public string szColumnName { get; set; }

        /// <summary>
        /// Gets or sets the options for the conditional index.
        /// </summary>
        public ConditionalColumnGrbit grbit { get; set; }

        /// <summary>
        /// Gets the NATIVE_CONDITIONALCOLUMN version of this object.
        /// </summary>
        /// <returns>A NATIVE_CONDITIONALCOLUMN for this object.</returns>
        internal NATIVE_CONDITIONALCOLUMN GetNativeConditionalColumn()
        {
            var native = new NATIVE_CONDITIONALCOLUMN();
            native.cbStruct = (uint) Marshal.SizeOf(native);
            native.szColumnName = this.szColumnName;
            native.grbit = (uint) this.grbit;
            return native;
        }
    }
}
