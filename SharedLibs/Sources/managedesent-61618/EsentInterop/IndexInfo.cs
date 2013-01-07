//-----------------------------------------------------------------------
// <copyright file="IndexInfo.cs" company="Microsoft Corporation">
//     Copyright (c) Microsoft Corporation.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.Isam.Esent.Interop
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Diagnostics;
    using System.Globalization;
    using System.Text;

    /// <summary>
    /// Information about one esent index. This is not an interop
    /// class, but is used by the meta-data helper methods.
    /// </summary>
    [Serializable]
    public class IndexInfo
    {
        /// <summary>
        /// The name of the index.
        /// </summary>
        private readonly string name;

        /// <summary>
        /// The culture info of the index.
        /// </summary>
        private readonly CultureInfo cultureInfo;

        /// <summary>
        /// Index comparison options.
        /// </summary>
        private readonly CompareOptions compareOptions;

        /// <summary>
        /// Index segments.
        /// </summary>
        private readonly ReadOnlyCollection<IndexSegment> indexSegments;

        /// <summary>
        /// Index options.
        /// </summary>
        private readonly CreateIndexGrbit grbit;

        /// <summary>
        /// Number of unique keys in the index.
        /// </summary>
        private readonly int keys;

        /// <summary>
        /// Number of entries in the index.
        /// </summary>
        private readonly int entries;

        /// <summary>
        /// Number of pages in the index.
        /// </summary>
        private readonly int pages;

        /// <summary>
        /// Initializes a new instance of the IndexInfo class.
        /// </summary>
        /// <param name="name">Name of the index.</param>
        /// <param name="cultureInfo">CultureInfo for string sorting.</param>
        /// <param name="compareOptions">String comparison options.</param>
        /// <param name="indexSegments">Array of index segment descriptions.</param>
        /// <param name="grbit">Index options.</param>
        /// <param name="keys">Number of unique keys in the index.</param>
        /// <param name="entries">Number of entries in the index.</param>
        /// <param name="pages">Number of pages in the index.</param>
        internal IndexInfo(
            string name,
            CultureInfo cultureInfo,
            CompareOptions compareOptions,
            IndexSegment[] indexSegments,
            CreateIndexGrbit grbit,
            int keys,
            int entries,
            int pages)
        {
            this.name = name;
            this.cultureInfo = cultureInfo;
            this.compareOptions = compareOptions;
            this.indexSegments = Array.AsReadOnly(indexSegments);
            this.grbit = grbit;
            this.keys = keys;
            this.entries = entries;
            this.pages = pages;
        }

        /// <summary>
        /// Gets the name of the index.
        /// </summary>
        public string Name
        {
            [DebuggerStepThrough]
            get { return this.name; }
        }

        /// <summary>
        /// Gets the CultureInfo the index is sorted by.
        /// </summary>
        public CultureInfo CultureInfo
        {
            [DebuggerStepThrough]
            get { return this.cultureInfo; }
        }

        /// <summary>
        /// Gets the CompareOptions for the index.
        /// </summary>
        public CompareOptions CompareOptions
        {
            [DebuggerStepThrough]
            get { return this.compareOptions; }
        }

        /// <summary>
        /// Gets the segments of the index.
        /// </summary>
        public IList<IndexSegment> IndexSegments
        {
            [DebuggerStepThrough]
            get { return this.indexSegments; }
        }

        /// <summary>
        /// Gets the index options.
        /// </summary>
        public CreateIndexGrbit Grbit
        {
            [DebuggerStepThrough]
            get { return this.grbit; }
        }

        /// <summary>
        /// Gets the number of unique keys in the index.
        /// This value is not current and is only is updated by <see cref="Api.JetComputeStats"/>.
        /// </summary>
        public int Keys
        {
            [DebuggerStepThrough]
            get { return this.keys; }
        }

        /// <summary>
        /// Gets the number of entries in the index.
        /// This value is not current and is only is updated by <see cref="Api.JetComputeStats"/>.
        /// </summary>
        public int Entries
        {
            [DebuggerStepThrough]
            get { return this.entries; }
        }

        /// <summary>
        /// Gets the number of pages in the index.
        /// This value is not current and is only is updated by <see cref="Api.JetComputeStats"/>.
        /// </summary>
        public int Pages
        {
            [DebuggerStepThrough]
            get { return this.pages; }
        }

        /// <summary>
        /// Returns a <see cref="T:System.String"/> that represents the current <see cref="IndexInfo"/>.
        /// </summary>
        /// <returns>
        /// A <see cref="T:System.String"/> that represents the current <see cref="IndexInfo"/>.
        /// </returns>
        public override string ToString()
        {
            StringBuilder sb = new StringBuilder();
            sb.Append(this.Name);
            sb.Append(" (");
            foreach (var segment in this.IndexSegments)
            {
                sb.Append(segment.ToString());
            }

            sb.Append(")");
            return sb.ToString();
        }
    }
}