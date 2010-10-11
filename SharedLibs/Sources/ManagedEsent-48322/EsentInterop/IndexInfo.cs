//-----------------------------------------------------------------------
// <copyright file="IndexInfo.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System.Globalization;

    /// <summary>
    /// Information about one esent index. This is not an interop
    /// class, but is used by the meta-data helper methods.
    /// </summary>
    public class IndexInfo
    {
        /// <summary>
        /// Initializes a new instance of the IndexInfo class.
        /// </summary>
        /// <param name="name">Name of the index.</param>
        /// <param name="cultureInfo">CultureInfo for string sorting.</param>
        /// <param name="compareOptions">String comparison options.</param>
        /// <param name="indexSegments">Array of index segment descriptions.</param>
        /// <param name="grbit">Index options.</param>
        internal IndexInfo(
            string name,
            CultureInfo cultureInfo,
            CompareOptions compareOptions,
            IndexSegment[] indexSegments,
            CreateIndexGrbit grbit)
        {
            this.Name = name;
            this.CultureInfo = cultureInfo;
            this.CompareOptions = compareOptions;
            this.IndexSegments = indexSegments;
            this.Grbit = grbit;
        }

        /// <summary>
        /// Gets the name of the index.
        /// </summary>
        public string Name { get; private set; }

        /// <summary>
        /// Gets the CultureInfo the index is sorted by.
        /// </summary>
        public CultureInfo CultureInfo { get; private set; }

        /// <summary>
        /// Gets the CompareOptions for the index.
        /// </summary>
        public CompareOptions CompareOptions { get; private set; }

        /// <summary>
        /// Gets the segments of the index.
        /// </summary>
        public IndexSegment[] IndexSegments { get; private set; }

        /// <summary>
        /// Gets the index options.
        /// </summary>
        public CreateIndexGrbit Grbit { get; private set; }
    }
}
