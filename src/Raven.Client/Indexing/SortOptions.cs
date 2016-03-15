//-----------------------------------------------------------------------
// <copyright file="SortOptions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Abstractions.Indexing
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

        /// <summary>Sort using term values as encoded Doubles.  Sort values are Double and
        /// lower values are at the front. 
        /// </summary>
        NumbericDouble = 7,

        /// <summary>Sort using term values as Strings, but comparing by
        /// value (using String.compareTo) for all comparisons.
        /// This is typically slower than {@link #STRING}, which
        /// uses ordinals to do the sorting. 
        /// </summary>
        StringVal = 11,

        /// <summary>
        /// Sort using term values as numbers according to types of blittable format (long / double)
        /// </summary>
        NumericDefault = 128
    }
}
