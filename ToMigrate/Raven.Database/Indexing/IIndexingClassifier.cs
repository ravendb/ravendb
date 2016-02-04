using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Database.Indexing
{
    public interface IIndexingClassifier
    {
        Dictionary<Etag, List<IndexToWorkOn>> GroupMapIndexes(IList<IndexToWorkOn> indexes);
    }
}
