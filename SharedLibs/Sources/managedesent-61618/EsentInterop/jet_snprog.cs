//-----------------------------------------------------------------------
// <copyright file="jet_snprog.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_SNPROG structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential, Pack = 0)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_SNPROG
    {
        /// <summary>
        /// Size of this structure.
        /// </summary>
        public static readonly int Size = Marshal.SizeOf(typeof(NATIVE_SNPROG));

        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// The number of work units that are already completed during the long
        /// running operation.
        /// </summary>
        public uint cunitDone;

        /// <summary>
        /// The number of work units that need to be completed. This value will
        /// always be bigger than or equal to cunitDone.
        /// </summary>
        public uint cunitTotal;
    }

    /// <summary>
    /// Contains information about the progress of a long-running operation.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    [Serializable]
    public class JET_SNPROG : IEquatable<JET_SNPROG>
    {
        /// <summary>
        /// Number of units of work that have completed.
        /// </summary>
        private int completedUnits;

        /// <summary>
        /// Total number of units of work to be done.
        /// </summary>
        private int totalUnits;

        /// <summary>
        /// Gets the number of work units that are already completed during the long
        /// running operation.
        /// </summary>
        public int cunitDone
        {
            [DebuggerStepThrough]
            get { return this.completedUnits; }
            internal set { this.completedUnits = value; }
        }

        /// <summary>
        /// Gets the number of work units that need to be completed. This value will
        /// always be bigger than or equal to cunitDone.
        /// </summary>
        public int cunitTotal
        {
            [DebuggerStepThrough]
            get { return this.totalUnits; }
            internal set { this.totalUnits = value; }
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="obj">An object to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public override bool Equals(object obj)
        {
            if (obj == null || this.GetType() != obj.GetType())
            {
                return false;
            }

            return this.Equals((JET_SNPROG)obj);
        }

        /// <summary>
        /// Generate a string representation of the instance.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_SNPROG({0}/{1})", this.cunitDone, this.cunitTotal);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return unchecked(this.cunitDone * 31) ^ this.cunitTotal;
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_SNPROG other)
        {
            if (null == other)
            {
                return false;
            }

            return this.cunitDone == other.cunitDone && this.cunitTotal == other.cunitTotal;
        }

        /// <summary>
        /// Set the members of this class from a <see cref="NATIVE_SNPROG"/>.
        /// </summary>
        /// <param name="native">The native struct.</param>
        internal void SetFromNative(NATIVE_SNPROG native)
        {
            Debug.Assert(native.cbStruct == NATIVE_SNPROG.Size, "NATIVE_SNPROG is the wrong size");
            this.cunitDone = checked((int)native.cunitDone);
            this.cunitTotal = checked((int)native.cunitTotal);
        }
    }
}
