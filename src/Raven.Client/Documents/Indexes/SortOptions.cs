//-----------------------------------------------------------------------
// <copyright file="SortOptions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// The sort options to use for a particular field
    /// </summary>
    public enum SortOptions
    {
        /// <summary>
        /// No sort options
        /// </summary>
        None = 0,
        /// <summary>Sort using term values as Strings.  Sort values are String and lower
        /// values are at the front. 
        /// </summary>
        String = 3,

        /// <summary>Sort using term values as encoded Doubles and Longs.  Sort values are Double or Longs and
        /// lower values are at the front. 
        /// </summary>
        Numeric = 7,

        /// <summary>Sort using term values as Strings, but comparing by
        /// value (using String.compareTo) for all comparisons.
        /// This is typically slower than {@link #STRING}, which
        /// uses ordinals to do the sorting. 
        /// </summary>
        StringVal = 11,

        AlphaNumeric = 255
    }
}
