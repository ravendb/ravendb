//-----------------------------------------------------------------------
// <copyright file="jet_bklogtime.cs" company="Microsoft Corporation">
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
    /// Describes a date/time when a backup occured.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1305:FieldNamesMustNotUseHungarianNotation",
        Justification = "This should match the name of the unmanaged structure.")]
    [StructLayout(LayoutKind.Sequential)]
    [Serializable]
    public struct JET_BKLOGTIME : IEquatable<JET_BKLOGTIME>, IJET_LOGTIME
    {
        /// <summary>
        /// The time in seconds. This value can be 0 to 59.
        /// </summary>
        private readonly byte bSeconds;

        /// <summary>
        /// The time in minutes. This value can be 0 to 59.
        /// </summary>
        private readonly byte bMinutes;

        /// <summary>
        /// The time in hours. This value can be 0 to 23.
        /// </summary>
        private readonly byte bHours;

        /// <summary>
        /// The day of the month. This value can be 0 to 31. 0 is
        /// used when the structure is null.
        /// </summary>
        private readonly byte bDays;

        /// <summary>
        /// The month. This value can be 0 to 12. 0 is
        /// used when the structure is null.
        /// </summary>
        private readonly byte bMonth;

        /// <summary>
        /// The year of the event, offset by 1900.
        /// </summary>
        private readonly byte bYear;

        /// <summary>
        /// This field is ignored.
        /// </summary>
        private readonly byte bFiller1;

        /// <summary>
        /// This field is ignored.
        /// </summary>
        private readonly byte bFiller2;

        /// <summary>
        /// Initializes a new instance of the <see cref="JET_BKLOGTIME"/> struct.
        /// </summary>
        /// <param name="time">
        /// The DateTime to intialize the structure with.
        /// </param>
        /// <param name="isSnapshot">
        /// True if this time is for a snapshot backup.
        /// </param>
        internal JET_BKLOGTIME(DateTime time, bool isSnapshot)
        {
            this.bSeconds = checked((byte)time.Second);
            this.bMinutes = checked((byte)time.Minute);
            this.bHours = checked((byte)time.Hour);
            this.bDays = checked((byte)time.Day);
            this.bMonth = checked((byte)time.Month);
            this.bYear = checked((byte)(time.Year - 1900));
            this.bFiller1 = (time.Kind == DateTimeKind.Utc) ? (byte)0x80 : (byte)0;
            this.bFiller2 = isSnapshot ? (byte)0x80 : (byte)0;
        }

        /// <summary>
        /// Gets a value indicating whether the JET_BKLOGTIME has a null value.
        /// </summary>
        public bool HasValue
        {
            get { return 0 != this.bMonth && 0 != this.bDays; }
        }

        /// <summary>
        /// Gets a value indicating whether the JET_BKLOGTIME is in UTC.
        /// </summary>
        public bool IsUtc
        {
            get { return 0 != (this.bFiller1 & 0x80); }
        }

        /// <summary>
        /// Gets a value indicating whether the JET_BKLOGTIME is for a snapshot backup.
        /// </summary>
        public bool IsSnapshot
        {
            get { return 0 != (this.bFiller2 & 0x80); }
        }

        /// <summary>
        /// Determines whether two specified instances of JET_BKLOGTIME
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_BKLOGTIME lhs, JET_BKLOGTIME rhs)
        {
            return lhs.Equals(rhs);
        }

        /// <summary>
        /// Determines whether two specified instances of JET_BKLOGTIME
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_BKLOGTIME lhs, JET_BKLOGTIME rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Generate a DateTime representation of this JET_BKLOGTIME.
        /// </summary>
        /// <returns>
        /// A DateTime representing the JET_BKLOGTIME. If the JET_BKLOGTIME
        /// is null then null is returned.
        /// </returns>
        public DateTime? ToDateTime()
        {
            if (!this.HasValue)
            {
                return null;
            }

            return new DateTime(
                this.bYear + 1900,
                this.bMonth,
                this.bDays,
                this.bHours,
                this.bMinutes,
                this.bSeconds,
                this.IsUtc ? DateTimeKind.Utc : DateTimeKind.Local);
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_BKLOGTIME({0}:{1}:{2}:{3}:{4}:{5}:0x{6:x}:0x{7:x})",
                this.bSeconds,
                this.bMinutes,
                this.bHours,
                this.bDays,
                this.bMonth,
                this.bYear,
                this.bFiller1,
                this.bFiller2);
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

            return this.Equals((JET_BKLOGTIME)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            // None of the members are larger than a byte and many use fewer than
            // all 8 bits (e.g. a month count uses only 4 bits). Spread the members
            // out through the 32-bit hash value.
            // (This is better than the default implementation of GetHashCode, which
            // easily aliases different JET_BKLOGTIMES to the same hash code)
            return this.bSeconds.GetHashCode()
                   ^ (this.bMinutes << 6)
                   ^ (this.bHours << 12)
                   ^ (this.bDays << 17)
                   ^ (this.bMonth << 22)
                   ^ (this.bYear << 24)
                   ^ this.bFiller1
                   ^ (this.bFiller2 << 8);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_BKLOGTIME other)
        {
            return this.bSeconds == other.bSeconds
                   && this.bMinutes == other.bMinutes
                   && this.bHours == other.bHours
                   && this.bDays == other.bDays
                   && this.bMonth == other.bMonth
                   && this.bYear == other.bYear
                   && this.IsUtc == other.IsUtc
                   && this.IsSnapshot == other.IsSnapshot;
        }
    }
}