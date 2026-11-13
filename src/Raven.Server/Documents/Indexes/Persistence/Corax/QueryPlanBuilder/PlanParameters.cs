using System;
using System.Collections.Generic;
using Corax.Mappings;
using Corax.Querying.Planning;
using Raven.Server.Documents.Queries;
using Sparrow.Json;
using Sparrow.Server;
using IndexSearcher = Corax.Querying.IndexSearcher;

namespace Raven.Server.Documents.Indexes.Persistence.Corax.QueryPlanBuilder;

internal class PlanParameters
{
    public ByteStringContext Allocator;
    public Lazy<List<string>> DynamicFields;
    public bool HasBoost;
    public bool HasDynamics;
    public Index Index;
    public IndexFieldsMapping IndexFieldsMapping;
    public IndexSearcher IndexSearcher;
    public QueryMetadata Metadata;
    public BlittableJsonReaderObject QueryParameters;

    // Human-readable label recorded on the plan bucket for diagnostics only
    public string CacheKey => Metadata.Query.QueryText;

    // The bucket for this plan, to avoid having to look for it again on miss 
    public PlanCache.PerQueryPlans Bucket;
    
    // Indexed by ValueOrdinal, array of how we can get the parameter value by index, after it was resolved for us on Build()
    public ParameterBinding[] SlotBindings;
}
