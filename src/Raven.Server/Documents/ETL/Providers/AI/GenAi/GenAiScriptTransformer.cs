using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Jint;
using Jint.Native;
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
    
    public GenAiScriptTransformer(DocumentDatabase database, DocumentsOperationContext context, Transformation transformation, PatchRequest behaviorFunctions, GenAiConfiguration configuration) : base(database, context, null, behaviorFunctions)
    {
        _configuration = configuration;
        _mainScript = new PatchRequest(transformation.Script, PatchRequestType.GenAi);
    }

    public override void Initialize(bool debugMode)
    {
        ReturnMainRun = Database.Scripts.GetScriptRunner(_mainScript, true, out DocumentScript);

        if (DocumentScript == null)
            return;

        if (debugMode)
            DocumentScript.DebugMode = true;

        var contextFunc = new ClrFunction(DocumentScript.ScriptEngine, "genContext", AddContext);
        ObjectInstance aiObject = new JsObject(DocumentScript.ScriptEngine);
        aiObject.FastSetProperty("genContext", new PropertyDescriptor(contextFunc, false, false, false));
        DocumentScript.ScriptEngine.SetValue("ai", aiObject);

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
        Current = item;
        _currentRun ??= [];

        Debug.Assert(item.IsDelete is false);
        
        DocumentScript.Run(Context, Context, "execute", [Current.Document]);
    }
    
    private JsValue AddContext(JsValue self, JsValue[] args)
    {
        const string methodDecl = "ai.genContext(ctx);";
        if (args.Length != 1)
            throw new InvalidOperationException($"Invalid number of arguments for {methodDecl}, got {args.Length} but expected 1.");

        if (args[0].IsObject() is false)
            throw new ArgumentException("Expected 'ctx' to be an object, but was: " + args[0].Type + ", " + args[0]);

        var context = JsBlittableBridge.Translate(Context, DocumentScript.ScriptEngine, args[0].AsObject());
        string hash = CalculateHash(context);
        var isCached = ShouldSendContext(hash, _configuration.Identifier, Current.Document) == false;

        using (context)
        {
            _currentRun.Add(new GenAiScriptResult(Current.DocumentId, context.CloneOnTheSameContext(), hash, isCached));
        }

        return JsValue.Null;
    }

    private static bool ShouldSendContext(string hash, string taskIdentifier, Document doc)
    {
        if (doc.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false ||
            metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection) == false ||
            hashesSection.TryGet(taskIdentifier, out BlittableJsonReaderArray existingHashes) == false)
            return true; // hash not found, should send

        foreach (var h in existingHashes)
        {
            if (string.Equals(hash, h?.ToString(), StringComparison.OrdinalIgnoreCase))
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
