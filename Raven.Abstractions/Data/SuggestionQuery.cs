//-----------------------------------------------------------------------
// <copyright file="SuggestionQuery.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Abstractions.Data
{
	/// <summary>
	/// 
	/// </summary>
	public class SuggestionQuery
	{
		/// <summary>
		/// Create a new instance of <seealso cref="SuggestionQuery"/>
		/// </summary>
		public SuggestionQuery()
		{
			MaxSuggestions = 15;
		}

		/// <summary>
		/// Gets or sets the term. The term is what the user likely entered, and will used as the basis of the suggestions.
		/// </summary>
		/// <value>The term.</value>
		public string Term { get; set; }
		/// <summary>
		/// Gets or sets the field to be used in conjunction with the index.
		/// </summary>
		/// <value>The field.</value>
		public string Field { get; set; }
		/// <summary>
		/// Gets or sets the number of suggestions to return.
		/// </summary>
		/// <value>The number of suggestions.</value>
		public int MaxSuggestions { get; set; }
		/// <summary>
		/// Gets or sets the string distance algorithm.
		/// </summary>
		/// <value>The distance.</value>
		public StringDistanceTypes Distance { get; set; }
		/// <summary>
		/// Gets or sets the accuracy.
		/// </summary>
		/// <value>The accuracy.</value>
		public float Accuracy { get; set; }
		/// <summary>
		/// Whatever to return the terms in order of popularity
		/// </summary>
		public bool Popularity { get; set; }
	}
}
