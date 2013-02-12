//-----------------------------------------------------------------------
// <copyright file="SuggestionQueryResult.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Abstractions.Data
{
	/// <summary>
	/// The result of the suggestion query
	/// </summary>
	public class SuggestionQueryResult
	{
		 /// <summary>
		/// The suggestions based on the term and dictionary
		/// </summary>
		/// <value>The suggestions.</value>
		public string[] Suggestions { get; set; } 
	}
}