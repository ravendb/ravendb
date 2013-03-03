//-----------------------------------------------------------------------
// <copyright file="jet_lgpos.cs" company="Microsoft Corporation">
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
    /// Describes an offset in the log sequence.
    /// </summary>
    [SuppressMessage(
        "Microsoft.StyleCop.CSharp.NamingRules",
        "SA1300:ElementMustBeginWithUpperCaseLetter",
        Justification = "This should match the name of the unmanaged structure.")]
    [StructLayout(LayoutKind.Sequential)]
    [Serializable]
    public struct JET_LGPOS : IEquatable<JET_LGPOS>, IComparable<JET_LGPOS>, INullableJetStruct
    {
        /// <summary>
        /// Byte offset inside the sector.
        /// </summary>
        private ushort offset;

        /// <summary>
        /// Sector number.
        /// </summary>
        private ushort sector;

        /// <summary>
        /// Generation number.
        /// </summary>
        private int generation;

        /// <summary>
        /// Gets the byte offset represented by this log position. This
        /// offset is inside of the sector.
        /// </summary>
        public int ib
        {
            [DebuggerStepThrough]
            get { return this.offset; }
            internal set { this.offset = checked((ushort)value); }
        }

        /// <summary>
        /// Gets the sector number represented by this log position.
        /// </summary>
        public int isec
        {
            [DebuggerStepThrough]
            get { return this.sector; }
            internal set { this.sector = checked((ushort)value); }
        }

        /// <summary>
        /// Gets the generation of this log position.
        /// </summary>
        public int lGeneration
        {
            [DebuggerStepThrough]
            get { return this.generation; }
            internal set { this.generation = value; }
        }

        /// <summary>
        /// Gets a value indicating whether this log position is null.
        /// </summary>
        public bool HasValue
        {
            get
            {
                return 0 != this.lGeneration;
            }
        }

        /// <summary>
        /// Determines whether two specified instances of JET_LGPOS
        /// are equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are equal.</returns>
        public static bool operator ==(JET_LGPOS lhs, JET_LGPOS rhs)
        {
            return lhs.Equals(rhs);
        }

        /// <summary>
        /// Determines whether two specified instances of JET_LGPOS
        /// are not equal.
        /// </summary>
        /// <param name="lhs">The first instance to compare.</param>
        /// <param name="rhs">The second instance to compare.</param>
        /// <returns>True if the two instances are not equal.</returns>
        public static bool operator !=(JET_LGPOS lhs, JET_LGPOS rhs)
        {
            return !(lhs == rhs);
        }

        /// <summary>
        /// Determine whether one log position is before another log position.
        /// </summary>
        /// <param name="lhs">The first log position to compare.</param>
        /// <param name="rhs">The second log position to compare.</param>
        /// <returns>True if lhs comes before rhs.</returns>
        public static bool operator <(JET_LGPOS lhs, JET_LGPOS rhs)
        {
            return lhs.CompareTo(rhs) < 0;
        }

        /// <summary>
        /// Determine whether one log position is after another log position.
        /// </summary>
        /// <param name="lhs">The first log position to compare.</param>
        /// <param name="rhs">The second log position to compare.</param>
        /// <returns>True if lhs comes after rhs.</returns>
        public static bool operator >(JET_LGPOS lhs, JET_LGPOS rhs)
        {
            return lhs.CompareTo(rhs) > 0;
        }

        /// <summary>
        /// Determine whether one log position is before or equal to
        /// another log position.
        /// </summary>
        /// <param name="lhs">The first log position to compare.</param>
        /// <param name="rhs">The second log position to compare.</param>
        /// <returns>True if lhs comes before or is equal to rhs.</returns>
        public static bool operator <=(JET_LGPOS lhs, JET_LGPOS rhs)
        {
            return lhs.CompareTo(rhs) <= 0;
        }

        /// <summary>
        /// Determine whether one log position is after or equal to
        /// another log position.
        /// </summary>
        /// <param name="lhs">The first log position to compare.</param>
        /// <param name="rhs">The second log position to compare.</param>
        /// <returns>True if lhs comes after or is equal to rhs.</returns>
        public static bool operator >=(JET_LGPOS lhs, JET_LGPOS rhs)
        {
            return lhs.CompareTo(rhs) >= 0;
        }

        /// <summary>
        /// Generate a string representation of the structure.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture,
                "JET_LGPOS(0x{0:X},{1:X},{2:X})",
                this.lGeneration,
                this.isec,
                this.ib);
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

            return this.Equals((JET_LGPOS)obj);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.generation ^ (this.sector << 16) ^ this.offset;
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(JET_LGPOS other)
        {
            return this.generation == other.generation
                   && this.sector == other.sector
                   && this.offset == other.offset;
        }

        /// <summary>
        /// Compares this log position to another log position and determines
        /// whether this instance is before, the same as or after the other
        /// instance.
        /// </summary>
        /// <param name="other">The log position to compare to the current instance.</param>
        /// <returns>
        /// A signed number indicating the relative positions of this instance and the value parameter.
        /// </returns>
        public int CompareTo(JET_LGPOS other)
        {
            int compare = this.generation.CompareTo(other.generation);
            if (0 == compare)
            {
                compare = this.sector.CompareTo(other.sector);
            }

            if (0 == compare)
            {
                compare = this.offset.CompareTo(other.offset);
            }

            return compare;
        }
    }
}