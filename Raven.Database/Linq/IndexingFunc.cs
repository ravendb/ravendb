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
}