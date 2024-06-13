using System;
using System.Collections.Generic;
using System.Threading;
using Corax.Querying;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.Timings;
using Sparrow;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results.Sharding;

public sealed class ShardedMapReduceResultRetriever : QueryResultRetrieverBase
{
    public ShardedMapReduceResultRetriever(ScriptRunnerCache scriptRunnerCache, IndexQueryServerSide query, QueryTimingsScope queryTimings, SearchEngineType searchEngineType, FieldsToFetch fieldsToFetch, DocumentsStorage documentsStorage, JsonOperationContext context, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand, char identitySeparator)
        : base(scriptRunnerCache, query, queryTimings, searchEngineType, fieldsToFetch, documentsStorage, context, reduceResults: true, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand, identitySeparator)
    {
    }

    public override (Document Document, List<Document> List) Get(ref RetrieverInput retrieverInput, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    public override bool TryGetKeyLucene(ref RetrieverInput retrieverInput, out string key)
    {
        throw new NotImplementedException();
    }

    public override bool TryGetKeyCorax(TermsReader searcher, long id, out UnmanagedSpan key)
    {
        throw new NotImplementedException();
    }

    public override Document DirectGet(ref RetrieverInput retrieverInput, string id, DocumentFields fields)
    {
        throw new NotImplementedException();
    }

    protected override Document LoadDocument(string id)
    {
        throw new NotImplementedException();
    }

    protected override long? GetCounter(string docId, string name)
    {
        throw new NotImplementedException();
    }

    protected override DynamicJsonValue GetCounterRaw(string docId, string name)
    {
        throw new NotImplementedException();
    }
}
