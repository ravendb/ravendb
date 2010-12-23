//-----------------------------------------------------------------------
// <copyright file="FieldIndexing.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
namespace Raven.Database.Indexing
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
		/// Expert: Index the field's value without an Analyzer, and also disable the storing of norms. 
		/// No norms means that index-time field and document boosting and field length normalization are disabled. 
		/// The benefit is less memory usage as norms take up one byte of RAM per indexed field for every document 
		/// in the index, during searching. 
		/// </summary>
		NotAnalyzedNoNorms,
		/// <summary>
		/// Index the tokens produced by running the field's value through an Analyzer. This is useful for common text.
		/// </summary>
		Analyzed,
		/// <summary>
		/// Index the field's value without using an Analyzer, so it can be searched.  As no analyzer is used the 
		/// value will be stored as a single term. This is useful for unique Ids like product numbers.
		/// </summary>
		NotAnalyzed
	}
}
