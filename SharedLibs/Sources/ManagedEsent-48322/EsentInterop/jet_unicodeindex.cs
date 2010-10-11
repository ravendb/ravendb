//-----------------------------------------------------------------------
// <copyright file="jet_unicodeindex.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Runtime.InteropServices;

    /// <summary>
    /// The native version of the JET_UNICODEINDEX structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_UNICODEINDEX
    {
        /// <summary>
        /// The LCID to be used when normalizing unicode data.
        /// </summary>
        public uint lcid;

        /// <summary>
        /// The flags for LCMapString.
        /// </summary>
        public uint dwMapFlags;
    }

    /// <summary>
    /// Customizes how Unicode data gets normalized when an index is created over a Unicode column.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    public class JET_UNICODEINDEX
    {
        /// <summary>
        /// Gets or sets the LCID to be used when normalizing unicode data.
        /// </summary>
        public int lcid { get; set; }

        /// <summary>
        /// Gets or sets the flags to be used with LCMapString when normalizing unicode data.
        /// </summary>
        [CLSCompliant(false)]
        public uint dwMapFlags { get; set; }

        /// <summary>
        /// Gets the native version of this object.
        /// </summary>
        /// <returns>The native version of this object.</returns>
        internal NATIVE_UNICODEINDEX GetNativeUnicodeIndex()
        {
            var native = new NATIVE_UNICODEINDEX
            {
                lcid = (uint) this.lcid,
                dwMapFlags = (uint) this.dwMapFlags,
            };
            return native;
        }        
    }
}
