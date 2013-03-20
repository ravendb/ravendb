//-----------------------------------------------------------------------
// <copyright file="jet_bkinfo.cs" company="Microsoft Corporation">
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
    /// Holds a collection of data about a specific backup event.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the name of the unmanaged structure.")]
    [StructLayout(LayoutKind.Sequential)]
    [Serializable]
    public struct JET_BKINFO : IEquatable<JET_BKINFO>, INullableJetStruct
    {
        /// <summary>
        /// Current log position. 
        /// </summary>
        private JET_LGPOS logPosition;

        /// <summary>
        /// Time the backup was made.
        /// </summary>
        private JET_BKLOGTIME backupTime;

        /// <summary>
        /// Low log generation when the backup was made.
        /// </summary>
        private uint lowGeneration;

        /// <summary>
        /// High log generation when the backup was made.
        /// </summary>
        private uint highGeneration;

        /// <summary>
        /// Gets the log position of the backup.
        /// </summary>
        public JET_LGPOS lgposMark
        {
            [DebuggerStepThrough]
            get { return this.logPosition; }
            internal set { this.logPosition = value; }
        }

        /// <summary>
        /// Gets the time of the backup.
        /// </summary>
        public JET_BKLOGTIME bklogtimeMark
        {
            [DebuggerStepThrough]
            get { return this.backupTime; }
            internal set { this.backupTime = value; }
        }

        /// <summary>
        /// Gets the low generation of the backup.
        /// </summary>
        public int genLow
        {
            [DebuggerStepThrough]
            get { return (int)this.lowGeneration; }
            internal set { this.lowGeneration = checked((uint)value); }
        }

        /// <summary>
        /// Gets or sets the high generation of the backup.
        /// </summary>
        public int genHigh
        {
            [DebuggerStepThrough]
            get { return (int)this.highGeneration; }
            set { this.highGeneration = checked((uint)value); }
        }

        /// <summary>
        /// Gets a value indicating whether this backup info is null.
        /// </summary>
        public bool HasValue
        {
            get
            {
                return this.lgposMark.HasValue
                       && this.backupTime.HasValue
                       && 0 != this.lowGeneration
                       && 0 != this.highGeneration;
            }
        }

        /// <summary>
        /// Determines whether two specified instances of JET_BKINFO
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_BKINFO lhs, JET_BKINFO rhs)
        {
            return lhs.Equals(rhs);
        }

        /// <summary>
        /// Determines whether two specified instances of JET_BKINFO
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_BKINFO lhs, JET_BKINFO rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_BKINFO({0}-{1}:{2}:{3})",
                this.genLow,
                this.genHigh,
                this.lgposMark,
                this.bklogtimeMark);
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

            return this.Equals((JET_BKINFO)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.logPosition.GetHashCode()
                   ^ this.backupTime.GetHashCode()
                   ^ unchecked((int)this.lowGeneration << 16)
                   ^ unchecked((int)this.lowGeneration >> 16)
                   ^ unchecked((int)this.highGeneration);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_BKINFO other)
        {
            return this.logPosition == other.logPosition
                   && this.backupTime == other.backupTime
                   && this.lowGeneration == other.lowGeneration
                   && this.highGeneration == other.highGeneration;
        }
    }
}