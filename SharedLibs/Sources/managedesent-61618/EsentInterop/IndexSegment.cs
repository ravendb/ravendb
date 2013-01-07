//-----------------------------------------------------------------------
// <copyright file="IndexSegment.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Diagnostics;
    using System.Globalization;

    /// <summary>
    /// Describes one segment of an index.
    /// </summary>
    [Serializable]
    public class IndexSegment : IEquatable<IndexSegment>
    {
        /// <summary>
        /// The name of the column.
        /// </summary>
        private readonly string columnName;

        /// <summary>
        /// The type of the column.
        /// </summary>
        private readonly JET_coltyp coltyp;

        /// <summary>
        /// True if the column is sorted in ascending order.
        /// </summary>
        private readonly bool isAscending;

        /// <summary>
        /// True if the column is an ASCII column.
        /// </summary>
        private readonly bool isASCII;

        /// <summary>
        /// Initializes a new instance of the IndexSegment class.
        /// </summary>
        /// <param name="name">The name of the indexed column.</param>
        /// <param name="coltyp">The type of the column.</param>
        /// <param name="isAscending">True if the column is ascending.</param>
        /// <param name="isASCII">True if the column is over an ASCII column.</param>
        internal IndexSegment(
            string name,
            JET_coltyp coltyp,
            bool isAscending,
            bool isASCII)
        {
            this.columnName = name;
            this.coltyp = coltyp;
            this.isAscending = isAscending;
            this.isASCII = isASCII;
        }

        /// <summary>
        /// Gets name of the column being indexed.
        /// </summary>
        public string ColumnName
        {
            [DebuggerStepThrough]
            get { return this.columnName; }
        }

        /// <summary>
        /// Gets the type of the column being indexed.
        /// </summary>
        public JET_coltyp Coltyp
        {
            [DebuggerStepThrough]
            get { return this.coltyp; }
        }

        /// <summary>
        /// Gets a value indicating whether the index segment is ascending.
        /// </summary>
        public bool IsAscending
        {
            [DebuggerStepThrough]
            get { return this.isAscending; }
        }

        /// <summary>
        /// Gets a value indicating whether the index segment is over an ASCII text
        /// column. This value is only meaningful for text column segments.
        /// </summary>
        public bool IsASCII
        {
            [DebuggerStepThrough]
            get { return this.isASCII; }
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

            return this.Equals((IndexSegment)obj);
        }

        /// <summary>
        /// Generate a string representation of the instance.
        /// </summary>
        /// <returns>The structure as a string.</returns>
        public override string ToString()
        {
            return String.Format(
                CultureInfo.InvariantCulture, "{0}{1}({2})", this.isAscending ? "+" : "-", this.columnName, this.coltyp);
        }

        /// <summary>
        /// Returns the hash code for this instance.
        /// </summary>
        /// <returns>The hash code for this instance.</returns>
        public override int GetHashCode()
        {
            return this.columnName.GetHashCode()
                ^ (int)this.coltyp * 31
                ^ (this.isAscending ? 0x10000 : 0x20000)
                ^ (this.isASCII ? 0x40000 : 0x80000);
        }

        /// <summary>
        /// Returns a value indicating whether this instance is equal
        /// to another instance.
        /// </summary>
        /// <param name="other">An instance to compare with this instance.</param>
        /// <returns>True if the two instances are equal.</returns>
        public bool Equals(IndexSegment other)
        {
            if (null == other)
            {
                return false;
            }

            return this.columnName.Equals(other.columnName, StringComparison.OrdinalIgnoreCase)
                   && this.coltyp == other.coltyp
                   && this.isAscending == other.isAscending
                   && this.isASCII == other.isASCII;
        }
    }
}