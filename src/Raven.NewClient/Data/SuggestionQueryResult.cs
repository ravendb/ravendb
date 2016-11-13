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
        /// Suggestions based on the term and dictionary
        /// </summary>
        public string[] Suggestions { get; set; } 
    }
}
