//-----------------------------------------------------------------------
// <copyright file="FieldIndexing.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Options for indexing a field
    /// </summary>
    public enum FieldIndexing
    {
        /// <summary>
        /// Do not index the field value. This field can thus not be searched, but one can still access its contents provided it is stored.
        /// </summary>
        No,

        /// <summary>
        /// Index the tokens produced by running the field's value through an Analyzer. This is useful for common text.
        /// </summary>
        Search,

        /// <summary>
        /// Index the field's value without using an Analyzer, so it can be searched.  As no analyzer is used the 
        /// value will be stored as a single term. This is useful for unique Ids like product numbers.
        /// </summary>
        Exact,

        /// <summary>
        /// Index this field using the default internal analyzer: LowerCaseKeywordAnalyzer
        /// </summary>
        Default
    }

    [Flags]
    public enum AutoFieldIndexing
    {
        /// <summary>
        /// Do not index the field value. This field can thus not be searched, but one can still access its contents provided it is stored.
        /// </summary>
        No = 0,

        /// <summary>
        /// Index the tokens produced by running the field's value through an Analyzer. This is useful for common text.
        /// </summary>
        Search = 1 << 1,

        /// <summary>
        /// Index the field's value without using an Analyzer, so it can be searched.  As no analyzer is used the 
        /// value will be stored as a single term. This is useful for unique Ids like product numbers.
        /// </summary>
        Exact = 1 << 2,

        /// <summary>
        /// Index the tokens produced by running the field's value through an Analyzer (same as Search),
        /// store them in index and track term vector positions and offsets. This is mandatory when highlighting is used.
        /// </summary>
        Highlighting = 1 << 3,

        /// <summary>
        /// Index this field using the default internal analyzer: LowerCaseKeywordAnalyzer
        /// </summary>
        Default = 1 << 4, 
    }
}
