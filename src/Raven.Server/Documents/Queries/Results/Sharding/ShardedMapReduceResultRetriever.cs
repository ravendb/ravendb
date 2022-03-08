using System;
using System.Collections.Generic;
using System.Threading;
using Lucene.Net.Search;
using Lucene.Net.Store;
using Raven.Server.Documents.Includes;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries.Timings;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Queries.Results.Sharding;

public class ShardedMapReduceResultRetriever : QueryResultRetrieverBase
{
    public ShardedMapReduceResultRetriever(ScriptRunnerCache scriptRunnerCache, IndexQueryServerSide query, QueryTimingsScope queryTimings, FieldsToFetch fieldsToFetch, DocumentsStorage documentsStorage, JsonOperationContext context, bool reduceResults, IncludeDocumentsCommand includeDocumentsCommand, IncludeCompareExchangeValuesCommand includeCompareExchangeValuesCommand, IncludeRevisionsCommand includeRevisionsCommand, char identitySeparator) : base(scriptRunnerCache, query, queryTimings, fieldsToFetch, documentsStorage, context, reduceResults, includeDocumentsCommand, includeCompareExchangeValuesCommand, includeRevisionsCommand, identitySeparator)
    {
    }

    public override (Document Document, List<Document> List) Get(Lucene.Net.Documents.Document input, ScoreDoc scoreDoc, IState state, CancellationToken token)
    {
        throw new NotImplementedException();
    }

    public override bool TryGetKey(Lucene.Net.Documents.Document document, IState state, out string key)
    {
        throw new NotImplementedException();
    }

    public override Document DirectGet(Lucene.Net.Documents.Document input, string id, DocumentFields fields, IState state)
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
