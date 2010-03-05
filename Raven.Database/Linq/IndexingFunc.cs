using System.Collections.Generic;

namespace Raven.Database.Linq
{
    /// <summary>
    /// Defining the indexing function for a set of documents
    /// </summary>
    public delegate IEnumerable<object> IndexingFunc(IEnumerable<object> source);
}