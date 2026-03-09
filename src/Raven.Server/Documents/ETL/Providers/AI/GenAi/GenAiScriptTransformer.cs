using System;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using Jint;
using Jint.Native;
using Jint.Native.Function;
using Jint.Native.Object;
using Raven.Client;
using Raven.Client.Documents.Operations.AI;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Providers.AI.GenAi.Stats;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow;
using Sparrow.Json;
using Sparrow.Platform;
using AttachmentsRequestConstants = Raven.Server.Documents.AI.ChatCompletionClient.Constants.AttachmentsRequestFields;

namespace Raven.Server.Documents.ETL.Providers.AI.GenAi;

internal sealed class GenAiScriptTransformer : EtlTransformer<GenAiItem, GenAiScriptResult, GenAiStatsScope, GenAiPerformanceOperation>
{
    private readonly GenAiConfiguration _configuration;
    private byte[] _configurationPartialHash;
    private List<GenAiScriptResult> _currentRun;
    private Dictionary<JsValue, Attachment> _attachments;
    private readonly GenAiStatsScope _stats;

    private static readonly string JavaScriptApi = @"
class AIContextItem {
  #withAttachment(type, data) {
    if (typeof type !== 'string' || (typeof data !== 'string' && data !== null)) {
        throw new Error('both type and data must be strings');
    }
    this.attachments.push({ type, data });
    return this;
  }

  constructor(ctx) {
    if (ctx !== null && (typeof ctx !== 'object' || Array.isArray(ctx))) {
      throw new Error('ctx must be an object');
    }
    this.ctx = ctx;
    this.attachments = [];
  }

  withText(data) {
    return this.#withAttachment('text/plain', data);
  }

  withJpeg(data) {
    return this.#withAttachment('image/jpeg', data);
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
      throw new Error('Invalid number of arguments for ai.genContext(ctx)');
    }
    const ctx = new AIContextItem(args[0]);
    this.#allContexts.push(ctx);
    return ctx;
  }
}

var ai = new AI();
"; 

    public GenAiScriptTransformer(DocumentDatabase database, DocumentsOperationContext context, Transformation transformation, PatchRequest behaviorFunctions, GenAiConfiguration configuration, GenAiStatsScope stats) 
        : base(database, context, new PatchRequest(transformation.Script, PatchRequestType.GenAi), behaviorFunctions)
    {
        _configuration = configuration;
        _stats = stats.For(EtlOperations.Transform, start: false);
    }

    public override void Dispose()
    {
        Context.CloseTransaction();
        base.Dispose();
    }

    public override void Initialize(bool debugMode)
    {
        _configurationPartialHash = GetInitialHash(_configuration);

        base.Initialize(debugMode);
        JsValue aiAlreadyExists = DocumentScript.ScriptEngine.GetValue("ai");
        if (aiAlreadyExists.IsNull() || aiAlreadyExists.IsUndefined())
        {
            DocumentScript.ScriptEngine.Execute(JavaScriptApi);
        }
    }

    protected override MissingAttachmentPolicy MissingAttachmentOnLoadPolicy => MissingAttachmentPolicy.ReturnEmpty;

    protected override void AddLoadedAttachment(JsValue reference, string name, Attachment attachment)
    {
        _attachments ??= [];
        _attachments.Add(reference, attachment);
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
        if (contexts.Length == 0)
        {
            // No contexts were generated for this document.
            // If metadata still contains GenAI hashes for this task, enqueue a marker so the load phase can remove them.
            if (Current.Document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) &&
                metadata.TryGet(Constants.Documents.Metadata.GenAiHashes, out BlittableJsonReaderObject hashesSection) &&
                hashesSection.TryGet(_configuration.Identifier, out object _))
            {
                _currentRun.Add(new GenAiScriptResult(
                    Current.DocumentId,
                    Context: null,
                    AiHash: null,
                    IsCached: true,
                    IsMetadataCleanupMarker: true));
            }

            return;
        }

        List<string> attachmentsHashes = null;
        foreach (var ctx in contexts)
        {
            attachmentsHashes?.Clear();
            
            ObjectInstance ctxObj = ctx.AsObject();
            ObjectInstance userSpecificCtx = ctxObj.GetOwnProperty("ctx").Value.AsObject();
            var context = JsBlittableBridge.Translate(Context, DocumentScript.ScriptEngine, userSpecificCtx);
            _stats.NumberOfContextObjects++;

            foreach (var current in ctxObj.GetOwnProperty("attachments").Value.AsArray())
            {
                attachmentsHashes ??= [];
                
                var attachmentObj = current.AsObject();
                var data = attachmentObj.GetOwnProperty("data").Value;
                if (data.IsNull() == false)
                    attachmentsHashes.Add(_attachments?.TryGetValue(data, out var attachment) is true ? attachment?.Base64Hash.ToString() : data.AsString());

                attachmentsHashes.Add(attachmentObj.GetOwnProperty(AttachmentsRequestConstants.Type).Value.AsString());
            }
            
            string hash = CalculateHash(context, attachmentsHashes);
            var isCached = ShouldSendContext(hash, _configuration.Identifier, Current.Document) == false;

            if (isCached)
                _stats.TotalCachedContexts++;

            using (context)
            {
                var result = new GenAiScriptResult(Current.DocumentId, context.CloneOnTheSameContext(), hash, isCached);
                if (attachmentsHashes?.Count > 0)
                {
                    result.Attachments = [];
                    
                    foreach (var current in ctxObj.GetOwnProperty("attachments").Value.AsArray())
                    {
                        var attachmentObj = current.AsObject();
                        var reference = attachmentObj.GetOwnProperty("data").Value;
                        var data = string.Empty;
                        string type = attachmentObj.GetOwnProperty(AttachmentsRequestConstants.Type).Value.AsString();
                        string filename = "unknown.name";
                        var source = AiAttachmentSource.FromAttachment;

                        // TODO: we aren't being really efficient here in terms of allocations / memory
                        // but the problem is that the API itself may require large BASE64 strings, annoying 
                        if (_attachments?.TryGetValue(reference, out var attachment) is true)
                        {
                            filename = attachment.Name.ToString(CultureInfo.InvariantCulture);
                            if (reference.IsNull())
                            {
                                source = AiAttachmentSource.NotFound;
                            }
                            else
                            {
                                data = DocumentScript.DebugMode ? GetAttachmentPreview(attachment, type) : GetAttachmentDataAsBase64(attachment, type);
                            }
                        }
                        else
                        {
                            //if we arrive here we probably didn't pass through loadAttachment() function
                            source = AiAttachmentSource.FromScript;
                            data = reference.ToString();
                            if (type != AttachmentsRequestConstants.MediaTypeTextPlain && IsBase64(data) == false)
                                throw new InvalidOperationException($"Attachment must be loaded or base64 string (on type {type})");
                        }

                        result.Attachments.Add(new AiAttachment(filename, type, source, data));
                    }
                }
                _currentRun.Add(result);
            }
        }
    }

    public static string GetAttachmentPreview(Attachment attachment, string type)
    {
        if (type == AttachmentsRequestConstants.MediaTypeTextPlain)
        {
            const int bytesSize = 100; 
            Span<byte> bytes = stackalloc byte[bytesSize];
            int bytesRead = attachment.Stream.Read(bytes);
            if (bytesRead <= 0)
                return string.Empty;

            var decoder = Encoding.UTF8.GetDecoder();
            Span<char> chars = stackalloc char[Encoding.UTF8.GetMaxCharCount(bytesSize)];
            int read = decoder.GetChars(bytes[..bytesRead], chars, flush: false);
            var s = new string(chars[..read]);

            if (bytesRead == bytesSize)
                return s + "...";

            return s;
        }

        return $"[Hash:'{attachment.Base64Hash}']";
    }

    public static string GetAttachmentDataAsBase64(Attachment attachment, string type)
    {
        using var memoryStream = RecyclableMemoryStreamFactory.GetRecyclableStream();
        if (type == AttachmentsRequestConstants.MediaTypeTextPlain)
        {
            attachment.Stream.CopyTo(memoryStream);
        }
        else // anything but text is using BASE64
        {
            using var transform = new ToBase64Transform();
            using var cryptoStream = new CryptoStream(attachment.Stream, transform, CryptoStreamMode.Read);
            cryptoStream.CopyTo(memoryStream);
        }

        Span<byte> readOnlySpan = memoryStream.GetBuffer();
        return Encoding.UTF8.GetString(readOnlySpan[..(int)memoryStream.Length]);
    }

    private static bool IsBase64(string data) => string.IsNullOrEmpty(data) == false && Base64.IsValid(data);
    
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
    private unsafe string CalculateHash(BlittableJsonReaderObject contextObj, List<string> attachmentsHashes)
    {
        var state = stackalloc byte[_configurationPartialHash.Length];
        _configurationPartialHash.CopyTo(new Span<byte>(state, _configurationPartialHash.Length));

        if (Sodium.crypto_generichash_update(state, contextObj.BasePointer, (ulong)contextObj.Size) != 0)
            ComputeHttpEtags.ThrowFailedToUpdateHash();

        foreach (string attachmentsHash in attachmentsHashes ?? [])
        {
            fixed(char* p = attachmentsHash)
            {
                if (Sodium.crypto_generichash_update(state, (byte*)p, (ulong)(sizeof(char) * attachmentsHash.Length)) != 0)
                    ComputeHttpEtags.ThrowFailedToUpdateHash();
            }
        }

        var hash = stackalloc byte[Sodium.GenericHashSize];
        if (Sodium.crypto_generichash_final(state, hash, Sodium.GenericHashSize) != 0)
            ComputeHttpEtags.ThrowFailedToUpdateHash();

        return Convert.ToBase64String(new ReadOnlySpan<byte>(hash, Sodium.GenericHashSize));
    }
}
