//-----------------------------------------------------------------------
// <copyright file="jet_rstmap.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Runtime.InteropServices;

    /// <summary>
    /// The native version of the JET_RSTMAP structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_RSTMAP
    {
        /// <summary>
        /// The old name/path of the database. Can be null if it is unchanged.
        /// </summary>
        public IntPtr szDatabaseName;

        /// <summary>
        ///  The current name/path of the database. Must not be null.
        /// </summary>
        public IntPtr szNewDatabaseName;

        /// <summary>
        /// Free the string memory.
        /// </summary>
        public void FreeHGlobal()
        {
            Marshal.FreeHGlobal(this.szDatabaseName);
            Marshal.FreeHGlobal(this.szNewDatabaseName);
        }
    }

    /// <summary>
    /// Enables the remapping of database file paths that are stored in the transaction logs during recovery.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    [Serializable]
    public class JET_RSTMAP : IContentEquatable<JET_RSTMAP>, IDeepCloneable<JET_RSTMAP>
    {
        /// <summary>
        /// The old name/path of the database. Can be null if it is unchanged.
        /// </summary>
        private string databaseName;

        /// <summary>
        ///  The current name/path of the database. Must not be null.
        /// </summary>
        private string newDatabaseName;

        /// <summary>
        /// Gets or sets the old name/path of the database. Can be null if it is unchanged.
        /// </summary>
        public string szDatabaseName
        {
            [DebuggerStepThrough]
            get { return this.databaseName; }
            set { this.databaseName = value; }
        }

        /// <summary>
        ///  Gets or sets the current name/path of the database. Must not be null.
        /// </summary>
        public string szNewDatabaseName
        {
            [DebuggerStepThrough]
            get { return this.newDatabaseName; }
            set { this.newDatabaseName = value; }
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_RSTMAP"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_RSTMAP"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_RSTINFO(szDatabaseName={0},szNewDatabaseName={1})",
                this.szDatabaseName,
                this.szNewDatabaseName);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool ContentEquals(JET_RSTMAP other)
        {
            if (null == other)
            {
                return false;
            }

            return String.Equals(this.szDatabaseName, other.szDatabaseName, StringComparison.OrdinalIgnoreCase)
                   && String.Equals(this.szNewDatabaseName, other.szNewDatabaseName, StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        public JET_RSTMAP DeepClone()
        {
            return (JET_RSTMAP)this.MemberwiseClone();
        }

        /// <summary>
        /// Get a native version of this managed structure.
        /// </summary>
        /// <returns>A native version of this object.</returns>
        internal NATIVE_RSTMAP GetNativeRstmap()
        {
            return new NATIVE_RSTMAP
            {
                // Don't pin this memory -- these structures are used by JetInit3,
                // which can run for a long time and we don't want to fragment the
                // heap. We do have to remember to free the memory though.
                szDatabaseName = Marshal.StringToHGlobalUni(this.szDatabaseName),
                szNewDatabaseName = Marshal.StringToHGlobalUni(this.szNewDatabaseName),
            };
        }
    }
}