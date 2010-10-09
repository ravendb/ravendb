using System.Collections.Generic;

namespace Raven.Database.Linq
{
	/// <summary>
	/// 	Defining the indexing function for a set of documents
	/// </summary>
	public delegate IEnumerable<dynamic> IndexingFunc(IEnumerable<dynamic> source);


	/// <summary>
	/// Get the group by value from the document
	/// </summary>
	public delegate dynamic GroupByKeyFunc(dynamic source);

    /// <summary>
    /// 	Defining the translator function for a set of results
    ///     about to sent to the user and apply final processing
    /// </summary>
    public delegate IEnumerable<dynamic> TranslatorFunc(ITranslatorDatabaseAccessor database, IEnumerable<dynamic> source);
}