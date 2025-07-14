using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Acornima.Ast;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Runtime.Descriptors;
using Jint.Runtime.Interop;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Platform;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

internal sealed class GenAiScriptTransformer : EtlTransformer<GenAiItem, GenAiScriptResult, GenAiStatsScope, GenAiPerformanceOperation>
{
    private readonly GenAiConfiguration _configuration;
    private byte[] _configurationPartialHash;
    private readonly PatchRequest _mainScript;
    private List<GenAiScriptResult> _currentRun;
    private readonly GenAiStatsScope _stats;

    private static readonly string JavaScriptApi = @"
class AIContextItem {
  #withAttachment(type, data) {
    if(!this.attachments) {
      this.attachments = [];
    }
    this.attachments.push({ type, data });
    return this;
  }

  constructor(ctx) {
    if (ctx !== null && (typeof ctx !== 'object' || Array.isArray(ctx))) {
      throw new Error('ctx must be an object');
    }
    this.ctx = ctx;
    this.attachments;
  }

  withText(data) {
    return this.#withAttachment('text/plain', data);
  }

  withPng(data) {
    return this.#withAttachment('image/png', data);
  }

  withWebp(data) {
    return this.#withAttachment('image/webp', data);
  }

  withGif(data) {
    return this.#withAttachment('image/gif', data);
  }

  withPdf(data) {
    return this.#withAttachment('application/pdf', data);
  }
}

class AI {
  #allContexts = [];

  __retrieveContexts() {
     const ctxs = this.#allContexts;
     this.#allContexts = [];
     return ctxs;
  }

  genContext(...args) {
    if (args.length !== 1) {
      throw new Error('invalid number of arguments, expected ai.genContext(ctx);');
    }
    const ctx = new AIContextItem(args[0]);
    this.#allContexts.push(ctx);
    return ctx;
  }
}

var ai = new AI();
"; 

    public GenAiScriptTransformer(DocumentDatabase database, DocumentsOperationContext context, Transformation transformation, PatchRequest behaviorFunctions, GenAiConfiguration configuration, GenAiStatsScope stats) : base(database, context, null, behaviorFunctions)
    {
        _configuration = configuration;
        _stats = stats.For(EtlOperations.Transform, start: false);
        _mainScript = new PatchRequest(transformation.Script, PatchRequestType.GenAi);
    }

    public override void Initialize(bool debugMode)
    {
        ReturnMainRun = Database.Scripts.GetScriptRunner(_mainScript, true, out DocumentScript);
        JsValue aiAlreadyExists = DocumentScript.ScriptEngine.GetValue("ai");
        if (aiAlreadyExists.IsNull() || aiAlreadyExists.IsUndefined())
        {
            DocumentScript.ScriptEngine.Execute(JavaScriptApi);
        }
        
        if (DocumentScript == null)
            return;

        if (debugMode)
            DocumentScript.DebugMode = true;

        _configurationPartialHash = GetInitialHash(_configuration);
    }

    protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
    {
        throw new NotSupportedException("Attachment are not supported in GenAI Task");
    }

    protected override void AddLoadedCounter(JsValue reference, string name, long value)
    {
        throw new NotSupportedException("Counters are not supported in GenAI Task");
    }

    protected override void AddLoadedTimeSeries(JsValue reference, string name, IEnumerable<SingleResult> entries)
    {
        throw new NotSupportedException("TimeSeries are not supported in GenAI Task");
    }

    protected override string[] LoadToDestinations { get; }

    protected override void LoadToFunction(string tableName, ScriptRunnerResult colsAsObject)
    {
        throw new NotSupportedException("loadTo() function is not supported in GenAI Task");
    }

    public override IEnumerable<GenAiScriptResult> GetTransformedResults()
    {
        return _currentRun ?? Enumerable.Empty<GenAiScriptResult>();
    }

    public override void Transform(GenAiItem item, GenAiStatsScope stats, EtlProcessState state)
    {
        using (_stats.Start())
        {
            Current = item;
            _currentRun ??= [];

            Debug.Assert(item.IsDelete is false);

            DocumentScript.Run(Context, Context, "execute", [Current.Document]);
            ProcessScriptResults();
        }
    }

    private void ProcessScriptResults()
    {
        ObjectInstance ai = DocumentScript.ScriptEngine.GetValue("ai").AsObject();
        Function retrieveContexts = ai.Prototype!.GetOwnProperty("__retrieveContexts").Value.AsFunctionInstance();
        JsArray contexts = retrieveContexts.Call(ai, []).AsArray();
        foreach (var ctx in contexts)
        {
            ObjectInstance ctxObj = ctx.AsObject();
            ObjectInstance userSpecifixCtx = ctxObj.GetOwnProperty("ctx").Value.AsObject();
            var context = JsBlittableBridge.Translate(Context, DocumentScript.ScriptEngine, userSpecifixCtx);
            _stats.NumberOfContextObjects++;

            string hash = CalculateHash(context);
            var isCached = ShouldSendContext(hash, _configuration.Identifier, Current.Document) == false;

            if (isCached)
                _stats.TotalCachedContexts++;

            using (context)
            {
                _currentRun.Add(new GenAiScriptResult(Current.DocumentId, context.CloneOnTheSameContext(), hash, isCached));
            }
        }
    }
    
    private static bool ShouldSendContext(string hash, string taskIdentifier, Document doc)
    {
        if (doc.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
            metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection) == false ||
            hashesSection.TryGet(taskIdentifier, out BlittableJsonReaderArray existingHashes) == false)
            return true; // hash not found, should send

        foreach (var h in existingHashes)
        {
            // those are base 64 values, they are case _sensitive_
            if (string.Equals(hash, h?.ToString(), StringComparison.Ordinal))
                return false; // already sent
        }

        return true; // hash not found, should send
    }

    private static unsafe byte[] GetInitialHash(GenAiConfiguration cfg)
    {
        var result = new byte[Sodium.crypto_generichash_statebytes()];
        fixed (byte* state = result)
        {
            if (Sodium.crypto_generichash_init(state, null, UIntPtr.Zero, Sodium.GenericHashSize) != 0)
                ComputeHttpEtags.ThrowFailToInitHash();

            UpdateHashString(state, cfg.Prompt);
            UpdateHashString(state, cfg.JsonSchema);
            UpdateHashString(state, cfg.UpdateScript);
            UpdateHashString(state, cfg.ConnectionStringName);
            return result;
        }

        static void UpdateHashString(byte* state, string str)
        {
            if (string.IsNullOrEmpty(str))
                return;

            fixed (char* p = str)
            {
                if (Sodium.crypto_generichash_update(state, (byte*)p, (ulong)(str.Length * sizeof(char))) != 0)
                    ComputeHttpEtags.ThrowFailedToUpdateHash();
            }
        }
    }

    [SkipLocalsInit]
    private unsafe string CalculateHash(BlittableJsonReaderObject contextObj)
    {
        var state = stackalloc byte[_configurationPartialHash.Length];
        _configurationPartialHash.CopyTo(new Span<byte>(state, _configurationPartialHash.Length));

        if (Sodium.crypto_generichash_update(state, contextObj.BasePointer, (ulong)contextObj.Size) != 0)
            ComputeHttpEtags.ThrowFailedToUpdateHash();

        var hash = stackalloc byte[Sodium.GenericHashSize];
        if (Sodium.crypto_generichash_final(state, hash, Sodium.GenericHashSize) != 0)
            ComputeHttpEtags.ThrowFailedToUpdateHash();

        return Convert.ToBase64String(new ReadOnlySpan<byte>(hash, Sodium.GenericHashSize));
    }
}
