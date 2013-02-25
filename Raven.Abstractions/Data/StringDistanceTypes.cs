//-----------------------------------------------------------------------
// <copyright file="StringDistanceTypes.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Abstractions.Data
{
	/// <summary>
	/// String distance algorithms used in suggestion query
	/// </summary>
	public enum StringDistanceTypes
	{
		/// <summary>
		/// Default, suggestion is not active
		/// </summary>
		None,

		/// <summary>
		/// Default, equivalent to Levenshtein
		/// </summary>
		Default,

		/// <summary>
		/// Levenshtein distance algorithm (default)
		/// </summary>
		Levenshtein,

		/// <summary>
		/// JaroWinkler distance algorithm
		/// </summary>
		JaroWinkler,
		
		/// <summary>
		/// NGram distance algorithm
		/// </summary>
		NGram,
	}
}
