//-----------------------------------------------------------------------
// <copyright file="jet_spacehints.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_SPACEHINTS structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_SPACEHINTS
    {
        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// Density at (append) layout.
        /// </summary>
        public uint ulInitialDensity;

        /// <summary>
        /// Initial size (in bytes).
        /// </summary>
        public uint cbInitial;

        /// <summary>
        /// Space hints options.
        /// </summary>
        public uint grbit;

        /// <summary>
        /// Density to maintain at.
        /// </summary>
        public uint ulMaintDensity;

        /// <summary>
        /// Percent growth from last growth or initial size (possibly rounded to nearest native JET allocation size).
        /// </summary>
        public uint ulGrowth;

        /// <summary>
        /// Overrides ulGrowth if too small.
        /// </summary>
        public uint cbMinExtent;

        /// <summary>
        /// Cap of ulGrowth.
        /// </summary>
        public uint cbMaxExtent;
    }

    /// <summary>
    /// Describes a column in a table of an ESENT database.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    [Serializable]
    public sealed class JET_SPACEHINTS : IContentEquatable<JET_SPACEHINTS>, IDeepCloneable<JET_SPACEHINTS>
    {
        /// <summary>
        /// Density at (append) layout.
        /// </summary>
        private int initialDensity;

        /// <summary>
        /// Initial size (in bytes).
        /// </summary>
        private int initialSize;

        /// <summary>
        /// Space hints options.
        /// </summary>
        private SpaceHintsGrbit options;

        /// <summary>
        /// Density to maintain at.
        /// </summary>
        private int maintenanceDensity;

        /// <summary>
        /// Percent growth from last growth or initial size (possibly rounded to nearest native JET allocation size).
        /// </summary>
        private int growthPercent;

        /// <summary>
        /// Overrides ulGrowth if too small.
        /// </summary>
        private int minimumExtent;

        /// <summary>
        /// Cap of ulGrowth.
        /// </summary>
        private int maximumExtent;
       
        /// <summary>
        /// Gets or sets the density at (append) layout.
        /// </summary>
        public int ulInitialDensity
        {
            [DebuggerStepThrough]
            get { return this.initialDensity; }
            set { this.initialDensity = value; }
        }

        /// <summary>
        /// Gets or sets the initial size (in bytes).
        /// </summary>
        public int cbInitial
        {
            [DebuggerStepThrough]
            get { return this.initialSize; }
            set { this.initialSize = value; }
        }

        /// <summary>
        /// Gets or sets the space hints options.
        /// </summary>
        public SpaceHintsGrbit grbit
        {
            [DebuggerStepThrough]
            get { return this.options; }
            set { this.options = value; }
        }

        /// <summary>
        /// Gets or sets the density at which to maintain.
        /// </summary>
        public int ulMaintDensity
        {
            [DebuggerStepThrough]
            get { return this.maintenanceDensity; }
            set { this.maintenanceDensity = value; }
        }

        /// <summary>
        /// Gets or sets the percent growth from last growth or initial size (possibly rounded
        /// to nearest native JET allocation size).
        /// Valid values are 0, and [100, 50000).
        /// </summary>
        public int ulGrowth
        {
            [DebuggerStepThrough]
            get { return this.growthPercent; }
            set { this.growthPercent = value; }
        }

        /// <summary>
        /// Gets or sets the value that overrides ulGrowth if too small. This value is in bytes.
        /// </summary>
        public int cbMinExtent
        {
            [DebuggerStepThrough]
            get { return this.minimumExtent; }
            set { this.minimumExtent = value; }
        }

        /// <summary>
        /// Gets or sets the value that sets the ceiling of ulGrowth. This value is in bytes.
        /// </summary>
        public int cbMaxExtent
        {
            [DebuggerStepThrough]
            get { return this.maximumExtent; }
            set { this.maximumExtent = value; }
        }

        #region IContentEquatable
        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool ContentEquals(JET_SPACEHINTS other)
        {
            if (null == other)
            {
                return false;
            }

            return this.ulInitialDensity == other.ulInitialDensity
                && this.cbInitial == other.cbInitial
                && this.grbit == other.grbit
                && this.ulMaintDensity == other.ulMaintDensity
                && this.ulGrowth == other.ulGrowth
                && this.cbMinExtent == other.cbMinExtent
                && this.cbMaxExtent == other.cbMaxExtent;
        }
        #endregion

        #region IDeepCloneable
        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        public JET_SPACEHINTS DeepClone()
        {
            JET_SPACEHINTS result = (JET_SPACEHINTS)this.MemberwiseClone();
            return result;
        }
        #endregion

        /// <summary>
        /// Generate a string representation of the instance.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(CultureInfo.InvariantCulture, "JET_SPACEHINTS({0})", this.grbit);
        }

        /// <summary>
        /// Returns the unmanaged columncreate that represents this managed class.
        /// </summary>
        /// <returns>A native (interop) version of the JET_SPACEHINTS.</returns>
        internal NATIVE_SPACEHINTS GetNativeSpaceHints()
        {
            var native = new NATIVE_SPACEHINTS();
            native.cbStruct = checked((uint)Marshal.SizeOf(native));

            native.ulInitialDensity = checked((uint)this.ulInitialDensity);
            native.cbInitial = checked((uint)this.cbInitial);
            native.grbit = (uint)this.grbit;
            native.ulMaintDensity = checked((uint)this.ulMaintDensity);
            native.ulGrowth = checked((uint)this.ulGrowth);
            native.cbMinExtent = checked((uint)this.cbMinExtent);
            native.cbMaxExtent = checked((uint)this.cbMaxExtent);

            return native;
        }

        /// <summary>
        /// Sets the fields of the object from a native JET_SPACEHINTS struct.
        /// </summary>
        /// <param name="value">
        /// The native columncreate to set the values from.
        /// </param>
        internal void SetFromNativeSpaceHints(NATIVE_SPACEHINTS value)
        {
            this.ulInitialDensity = checked((int)value.ulInitialDensity);
            this.cbInitial = checked((int)value.cbInitial);
            this.grbit = (SpaceHintsGrbit)value.grbit;
            this.ulMaintDensity = checked((int)value.ulMaintDensity);
            this.ulGrowth = checked((int)value.ulGrowth);
            this.cbMinExtent = checked((int)value.cbMinExtent);
            this.cbMaxExtent = checked((int)value.cbMaxExtent);
        }
    }
}