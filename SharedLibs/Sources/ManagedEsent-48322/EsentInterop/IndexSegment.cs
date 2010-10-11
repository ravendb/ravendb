//-----------------------------------------------------------------------
// <copyright file="IndexSegment.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    /// <summary>
    /// Describes one segment of an index.
    /// </summary>
    public class IndexSegment
    {
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
            this.ColumnName = name;
            this.Coltyp = coltyp;
            this.IsAscending = isAscending;
            this.IsASCII = isASCII;
        }

        /// <summary>
        /// Gets name of the column being indexed.
        /// </summary>
        public string ColumnName { get; private set; }

        /// <summary>
        /// Gets the type of the column being indexed.
        /// </summary>
        public JET_coltyp Coltyp { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the index segment is ascending.
        /// </summary>
        public bool IsAscending { get; private set; }

        /// <summary>
        /// Gets a value indicating whether the index segment is over an ASCII text
        /// column. This value is only meaningful for text column segments.
        /// </summary>
        public bool IsASCII { get; private set; }
    }
}
