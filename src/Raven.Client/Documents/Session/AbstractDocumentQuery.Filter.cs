using System.Collections.Generic;

namespace Raven.Client.Documents.Session;

public abstract partial class AbstractDocumentQuery<T, TSelf>
{
    private string _condition = string.Empty;
    protected HashSet<string> DocumentFilters = new HashSet<string>();

    /// <summary>
    /// Filter documents on specific condition without building new the index.
    /// </summary>
    /// <param name="condition">javascript conditional statement</param>
    public void Filter(string condition)
    {
        DocumentFilters.Add(condition);
    }
}
