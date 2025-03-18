using System.Linq;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Queries;

namespace Raven.Server.Documents.Indexes.Auto;

public static class IndexSearchEngineHelper
{
    public static SearchEngineType GetSearchEngineType(IndexQueryServerSide query, SearchEngineType defaultSearchEngineType)
    {
        if (query.Metadata.HasVectorSearch)
            return SearchEngineType.Corax;

        return defaultSearchEngineType;
    }

    public static SearchEngineType GetSearchEngineType(Index index)
    {
        return index.Type.IsAuto() switch
        {
            // We only support Vectors in Corax, so if an auto-index is using it, let's already set it up as such, regardless
            // of what type of default storage engine is configured.
            true when index.Definition.IndexFields.Any(x=> x.Value.Vector != null) => SearchEngineType.Corax,
            true => index.Configuration.AutoIndexingEngineType,
            false => index.Configuration.StaticIndexingEngineType
        };
    }
}
