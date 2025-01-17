using System;
using System.Collections.Generic;
using System.Linq;
using Jint.Native;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.AI;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL.Providers.AI;

public sealed class AiEtlDocumentTransformer : EtlTransformer<AiEtlItem, KeyValuePair<string, Dictionary<string, List<string>>>, EtlStatsScope, EtlPerformanceOperation>
{
    private readonly Transformation _transformation;
    private readonly AiEtlConfiguration _configuration;
    private AiEtlScriptRun _currentRun;
    
    public AiEtlDocumentTransformer(DocumentDatabase database, DocumentsOperationContext context, PatchRequest mainScript, PatchRequest behaviorFunctions, AiEtlConfiguration configuration) : base(database, context, mainScript, behaviorFunctions)
    {
        _configuration = configuration;
    }

    public override void Initialize(bool debugMode)
    {
        base.Initialize(debugMode);

        if (DocumentScript == null)
            return;
    }

    protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
    {
        throw new System.NotImplementedException();
    }

    protected override void AddLoadedCounter(JsValue reference, string name, long value)
    {
        throw new System.NotImplementedException();
    }

    protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
    {
        throw new System.NotImplementedException();
    }

    protected override string[] LoadToDestinations { get; }
    protected override void LoadToFunction(string tableName, ScriptRunnerResult colsAsObject)
    {
        throw new System.NotImplementedException();
    }

    /// docId -> <fieldName, <fieldValues>>
    public override IEnumerable<KeyValuePair<string, Dictionary<string, List<string>>>> GetTransformedResults()
    {
        return _currentRun?.CurrentRun ?? Enumerable.Empty<KeyValuePair<string, Dictionary<string, List<string>>>>();
    }

    public override void Transform(AiEtlItem item, EtlStatsScope stats, EtlProcessState state)
    {
        Current = item;
        _currentRun ??= new AiEtlScriptRun();
        
        var result = new Dictionary<string, List<string>>();
        
        foreach (var fieldName in _configuration.FieldsToInclude)
        {
            if (BlittableJsonTraverserHelper.TryRead(BlittableJsonTraverser.Default, item.Document, fieldName, out var fieldValue) == false)
                continue;
            
            if (fieldValue is LazyStringValue lsv)
                result.Add(fieldName, new List<string>() { lsv });
            // todo lazy
            else if (fieldValue is List<string> list)
                result.Add(fieldName, list);
            else
                throw new Exception();
        }

        _currentRun.Add(item.DocumentId, result);
    }
}
