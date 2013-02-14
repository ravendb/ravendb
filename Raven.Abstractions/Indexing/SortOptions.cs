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
		
		/// <summary>Sort using term values as encoded Integers.  Sort values are Integer and
		/// lower values are at the front. 
		/// </summary>
		Int = 4,
		
		/// <summary>Sort using term values as encoded Floats.  Sort values are Float and
		/// lower values are at the front. 
		/// </summary>
		Float = 5,
		
		/// <summary>Sort using term values as encoded Longs.  Sort values are Long and
		/// lower values are at the front. 
		/// </summary>
		Long = 6,
		
		/// <summary>Sort using term values as encoded Doubles.  Sort values are Double and
		/// lower values are at the front. 
		/// </summary>
		Double = 7,
		
		/// <summary>Sort using term values as encoded Shorts.  Sort values are Short and
		/// lower values are at the front. 
		/// </summary>
		Short = 8,
		
		/// <summary>Sort using a custom Comparator.  Sort values are any Comparable and
		/// sorting is done according to natural order. 
		/// </summary>
		Custom = 9,
		
		/// <summary>Sort using term values as encoded Bytes.  Sort values are Byte and
		/// lower values are at the front. 
		/// </summary>
		Byte = 10,
		
		/// <summary>Sort using term values as Strings, but comparing by
		/// value (using String.compareTo) for all comparisons.
		/// This is typically slower than {@link #STRING}, which
		/// uses ordinals to do the sorting. 
		/// </summary>
		StringVal = 11,
		
	}
}
