//-----------------------------------------------------------------------
// <copyright file="jet_rstinfo.cs" company="Microsoft Corporation">
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
    /// The native version of the JET_RSTINFO structure.
    /// </summary>
    [StructLayout(LayoutKind.Sequential)]
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1307:AccessibleFieldsMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    internal struct NATIVE_RSTINFO
    {
        /// <summary>
        /// The size of a NATIVE_RSTINFO structure.
        /// </summary>
        public static readonly int SizeOfRstinfo = Marshal.SizeOf(typeof(NATIVE_RSTINFO));

        /// <summary>
        /// Size of the structure.
        /// </summary>
        public uint cbStruct;

        /// <summary>
        /// The array of <see cref="NATIVE_RSTMAP"/> structures.
        /// </summary>
        public unsafe NATIVE_RSTMAP* rgrstmap;

        /// <summary>
        /// The number of elements in the restore-map array.
        /// </summary>
        public uint crstmap;

        /// <summary>
        /// The log position at which it stopped.
        /// </summary>
        public JET_LGPOS lgposStop;

        /// <summary>
        /// The time at which it stopped.
        /// </summary>
        public JET_LOGTIME logtimeStop;

        /// <summary>
        /// The callback to the status function.
        /// </summary>
        public NATIVE_PFNSTATUS pfnStatus;
    }

    /// <summary>
    /// Contains optional input and output parameters for JetRetrieveColumn.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the unmanaged API, which isn't capitalized.")]
    [Serializable]
    public class JET_RSTINFO : IContentEquatable<JET_RSTINFO>, IDeepCloneable<JET_RSTINFO>
    {
        /// <summary>
        /// Gets or sets the array of <see cref="JET_RSTMAP"/> structures.
        /// </summary>
        public JET_RSTMAP[] rgrstmap { get; set; }

        /// <summary>
        /// Gets or sets the number of elements in the restore-map array.
        /// </summary>
        public int crstmap { get; set; }

        /// <summary>
        /// Gets or sets the log position to stop recovery at.
        /// </summary>
        public JET_LGPOS lgposStop { get; set; }

        /// <summary>
        /// Gets or sets the time at which it stopped.
        /// </summary>
        public JET_LOGTIME logtimeStop { get; set; }

        /// <summary>
        /// Gets or sets the callback to Gets or sets the status function.
        /// </summary>
        public JET_PFNSTATUS pfnStatus { get; set; }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="JET_RSTINFO"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="JET_RSTINFO"/>.
        /// </returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_RSTINFO(crstmap={0})",
                this.crstmap);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool ContentEquals(JET_RSTINFO other)
        {
            if (null == other)
            {
                return false;
            }

            this.CheckMembersAreValid();
            other.CheckMembersAreValid();
            return this.crstmap == other.crstmap
                   && this.lgposStop == other.lgposStop
                   && this.logtimeStop == other.logtimeStop
                   && this.pfnStatus == other.pfnStatus
                   && Util.ArrayObjectContentEquals(this.rgrstmap, other.rgrstmap, this.crstmap);
        }

        /// <summary>
        /// Returns a deep copy of the object.
        /// </summary>
        /// <returns>A deep copy of the object.</returns>
        public JET_RSTINFO DeepClone()
        {
            var result = (JET_RSTINFO)this.MemberwiseClone();
            result.rgrstmap = Util.DeepCloneArray(this.rgrstmap);
            return result;
        }

        /// <summary>
        /// Check this object to make sure its parameters are valid.
        /// </summary>
        internal void CheckMembersAreValid()
        {
            if (this.crstmap < 0)
            {
                throw new ArgumentOutOfRangeException("crstmap", this.crstmap, "cannot be negative");                
            }

            if (null == this.rgrstmap && this.crstmap > 0)
            {
                throw new ArgumentOutOfRangeException("crstmap", this.crstmap, "must be zero");
            }

            if (null != this.rgrstmap && this.crstmap > this.rgrstmap.Length)
            {
                throw new ArgumentOutOfRangeException(
                    "crstmap",
                    this.crstmap,
                    "cannot be greater than the length of rgrstmap");
            }
        }

        /// <summary>
        /// Get a native version of this managed structure.
        /// </summary>
        /// <returns>A native version of this object.</returns>
        internal NATIVE_RSTINFO GetNativeRstinfo()
        {
            this.CheckMembersAreValid();
            var native = new NATIVE_RSTINFO
            {
                cbStruct = (uint)NATIVE_RSTINFO.SizeOfRstinfo,
                crstmap = checked((uint)this.crstmap),
                lgposStop = this.lgposStop,
                logtimeStop = this.logtimeStop
            };

            return native;
        }
    }
}