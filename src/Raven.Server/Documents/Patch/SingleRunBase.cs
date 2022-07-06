using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Jint.Native.Function;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Extensions.Jint;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Spatial4n.Core.Distance;
using V8.Net;

namespace Raven.Server.Documents.Patch;

public abstract class SingleRunBase
{
    public const string GetMetadataMethod = "getMetadata";
}
public abstract class SingleRun<T> : SingleRunBase, ISingleRun
    where T : struct, IJsHandle<T>
{

    public void CleanStuff()
    {
        ReadOnly = false;

        DebugMode = false;
        DebugOutput?.Clear();
        DebugActions?.Clear();
        IncludeRevisionsChangeVectors?.Clear();
        IncludeRevisionByDateTimeBefore = null;

        Includes?.Clear();
        CompareExchangeValueIncludes?.Clear();

        OriginalDocumentId = null;
        RefreshOriginalDocument = false;

        DocumentCountersToUpdate?.Clear();
        DocumentTimeSeriesToUpdate?.Clear();
        CleanInternal();
    }

    public IJsEngineHandle<T> EngineHandle;
    public JavaScriptUtilsBase<T> JsUtils;

    protected readonly DocumentDatabase _database;
    protected readonly RavenConfiguration _configuration;
    protected JavaScriptEngineType _jsEngineType;

    protected readonly ScriptRunner<T> _runner;
    protected QueryTimingsScope _scope;
    protected QueryTimingsScope _loadScope;
    protected DocumentsOperationContext _docsCtx;
    protected JsonOperationContext _jsonCtx;
    public PatchDebugActions DebugActions;
    public bool DebugMode { get; set; }
    public List<string> DebugOutput { get; set; }
    public bool PutOrDeleteCalled;
    public HashSet<string> Includes { get; set; }
    public HashSet<string> IncludeRevisionsChangeVectors { get; set; }
    public DateTime? IncludeRevisionByDateTimeBefore { get; set; }
    public HashSet<string> CompareExchangeValueIncludes { get; set; }
    protected HashSet<string> _documentIds;
    protected CancellationToken _token;
    public JsBlittableBridge<T> JsBlittableBridge;
    public bool ReadOnly
    {
        get => JsUtils.ReadOnly;
        set => JsUtils.ReadOnly = value;
    }
    IScriptEngineChanges ISingleRun.ScriptEngineHandle
    {
        get => EngineHandle;
        set
        {
        }
    }

    public string OriginalDocumentId;
    public bool RefreshOriginalDocument;
    protected readonly ConcurrentLruRegexCache _regexCache = new ConcurrentLruRegexCache(1024);
    public HashSet<string> DocumentCountersToUpdate { get; set; }
    public HashSet<string> DocumentTimeSeriesToUpdate { get; set; }

    protected const string _timeSeriesSignature = "timeseries(doc, name)";


    private List<string> _scriptsSource;

    protected SingleRun(DocumentDatabase database, RavenConfiguration configuration, ScriptRunner<T> runner, List<string> scriptsSource)
    {
        _database = database;
        _configuration = configuration;
        _jsEngineType = configuration.JavaScript.EngineType;
        _runner = runner;
        _scriptsSource = scriptsSource;
    }

    public void Initialize(bool executeScriptsSource = true)
    {
        EngineHandle.SetGlobalClrCallBack("getMetadata", JsUtils.GetMetadata);
        EngineHandle.SetGlobalClrCallBack("metadataFor", (JsUtils.GetMetadata));
        EngineHandle.SetGlobalClrCallBack("id", JsUtils.GetDocumentId);

        //console.log
        var consoleObject = EngineHandle.CreateObject();
        var jsFuncLog = EngineHandle.CreateClrCallBack("log", OutputDebug, keepAlive: true);
        consoleObject.FastAddProperty("log", jsFuncLog, false, false, false);
        EngineHandle.SetGlobalProperty("console", consoleObject);

        //spatial.distance
        var spatialObject = EngineHandle.CreateObject();
        var jsFuncSpatial = EngineHandle.CreateClrCallBack("distance", Spatial_Distance, keepAlive: true);
        spatialObject.FastAddProperty("distance", jsFuncSpatial.Clone(), false, false, false);
        EngineHandle.SetGlobalProperty("spatial", spatialObject);
        EngineHandle.SetGlobalProperty("spatial.distance", jsFuncSpatial);

        // includes
        var includesObject = EngineHandle.CreateObject();
        var jsFuncIncludeDocument = EngineHandle.CreateClrCallBack("include", IncludeDoc, keepAlive: true);
        includesObject.FastAddProperty("document", jsFuncIncludeDocument.Clone(), false, false, false);
        // includes - backward compatibility
        EngineHandle.SetGlobalProperty("include", jsFuncIncludeDocument);

        var jsFuncIncludeCompareExchangeValue =
            EngineHandle.CreateClrCallBack("cmpxchg", IncludeCompareExchangeValue, keepAlive: true);
        includesObject.FastAddProperty("cmpxchg", jsFuncIncludeCompareExchangeValue, false, false, false);

        var jsFuncIncludeRevisions = EngineHandle.CreateClrCallBack("revisions", IncludeRevisions, keepAlive: true);
        includesObject.FastAddProperty("revisions", jsFuncIncludeRevisions, false, false, false);
        EngineHandle.SetGlobalProperty("includes", includesObject);

        EngineHandle.SetGlobalClrCallBack("output", OutputDebug);
        EngineHandle.SetGlobalClrCallBack("load", LoadDocument);
        EngineHandle.SetGlobalClrCallBack("LoadDocument", ThrowOnLoadDocument);

        EngineHandle.SetGlobalClrCallBack("loadPath", LoadDocumentByPath);
        EngineHandle.SetGlobalClrCallBack("del", DeleteDocument);
        EngineHandle.SetGlobalClrCallBack("DeleteDocument", ThrowOnDeleteDocument);
        EngineHandle.SetGlobalClrCallBack("put", PutDocument);
        EngineHandle.SetGlobalClrCallBack("PutDocument", ThrowOnPutDocument);
        EngineHandle.SetGlobalClrCallBack("cmpxchg", CompareExchange);

        EngineHandle.SetGlobalClrCallBack("counter", GetCounter);
        EngineHandle.SetGlobalClrCallBack("counterRaw", GetCounterRaw);
        EngineHandle.SetGlobalClrCallBack("incrementCounter", IncrementCounter);
        EngineHandle.SetGlobalClrCallBack("deleteCounter", DeleteCounter);

        EngineHandle.SetGlobalClrCallBack("lastModified", GetLastModified);

        EngineHandle.SetGlobalClrCallBack("startsWith", StartsWith);
        EngineHandle.SetGlobalClrCallBack("endsWith", EndsWith);
        EngineHandle.SetGlobalClrCallBack("regex", Regex);

        EngineHandle.SetGlobalClrCallBack("Raven_ExplodeArgs", ExplodeArgs);
        EngineHandle.SetGlobalClrCallBack("Raven_Min", Raven_Min);
        EngineHandle.SetGlobalClrCallBack("Raven_Max", Raven_Max);

        EngineHandle.SetGlobalClrCallBack("convertJsTimeToTimeSpanString", ConvertJsTimeToTimeSpanString);
        EngineHandle.SetGlobalClrCallBack("convertToTimeSpanString", ConvertToTimeSpanString);
        EngineHandle.SetGlobalClrCallBack("compareDates", CompareDates);

        EngineHandle.SetGlobalClrCallBack("toStringWithFormat", ToStringWithFormat);

        EngineHandle.SetGlobalClrCallBack("scalarToRawString", ScalarToRawString);

        //TimeSeries
        EngineHandle.SetGlobalClrCallBack("timeseries", TimeSeries);
        EngineHandle.Execute(ScriptRunnerCache.PolyfillJs, "polyfill.js");

        if (executeScriptsSource)
        {
            ExecuteScriptsSource();
        }

        foreach (var ts in _runner.TimeSeriesDeclaration)
        {
            EngineHandle.SetGlobalClrCallBack(ts.Key,
                (
                    (self, args) => InvokeTimeSeriesFunction(ts.Key, args)
                )
            );
        }
    }

    public void ExecuteScriptsSource()
    {
        foreach (var script in _scriptsSource)
        {
            try
            {
                EngineHandle.Execute(script);
            }
            catch (Exception e)
            {
                throw new JavaScriptParseException("Failed to parse: " + Environment.NewLine + script, e);
            }
        }
    }

    protected abstract T CreateErrorAndSetLastExceptionIfNeeded(Exception e, JSValueType errorType);
    
    public T LoadDocument(T self, T[] args)
    {
        using (_loadScope = _loadScope?.Start() ?? _scope?.For(nameof(QueryTimingsScope.Names.Load)))
        {
            if (AssertValidDatabaseContext("load", out var val) == false)
            {
                return val;
            }

            if (args.Length != 1)
                return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"load(id | ids) must be called with a single string argument"), JSValueType.ExecutionError);

            if (args[0].IsNull || args[0].IsUndefined)
                return args[0];

            if (args[0].IsArray)
            {
                //TODO: egor create a emty array (ScriptEngineHandle.CreateEmptyArray) and push directly into it to save allocations
                var results = new List<T>();
                var jsArray = args[0];
                int arrayLength = jsArray.ArrayLength;
                for (int i = 0; i < arrayLength; ++i)
                {
                    using (var jsItem = jsArray.GetProperty(i))
                    {
                        if (jsItem.IsStringEx == false)
                            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("load(ids) must be called with a array of strings, but got " + jsItem.ValueType + " - " + jsItem), JSValueType.ExecutionError);

                        results.Add(LoadDocumentInternal(jsItem.AsString));
                    }
                }
                
                return EngineHandle.CreateArray(results);
            }

            if (args[0].IsStringEx == false)
                return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("load(id | ids) must be called with a single string or array argument"), JSValueType.ExecutionError);

            return LoadDocumentInternal(args[0].AsString);
        }
    }

    public T LoadDocumentByPath(T self, T[] args)
    {
        using (_loadScope = _loadScope?.Start() ?? _scope?.For(nameof(QueryTimingsScope.Names.Load)))
        {
            if (AssertValidDatabaseContext(("loadPath"), out var val) == false)
            {
                return val;
            }

            if (args.Length != 2 ||
                (args[0].IsNull == false && args[0].IsUndefined == false && args[0].IsObject == false)
                || args[1].IsString == false)
                return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("loadPath(doc, path) must be called with a document and path"), JSValueType.ExecutionError);

            if (args[0].IsNull || args[1].IsUndefined)
                return args[0];

            if (args[0].AsObject() is IBlittableObjectInstance<T> b)
            {
                var path = args[1].AsString;
                if (_documentIds == null)
                    _documentIds = new HashSet<string>();

                _documentIds.Clear();
                try
                {
                    IncludeUtil.GetDocIdFromInclude(b.Blittable, path, _documentIds, _database.IdentityPartsSeparator);
                }
                catch (Exception e)
                {
                    return CreateErrorAndSetLastExceptionIfNeeded(e, JSValueType.ExecutionError);
                }

                if (path.IndexOf("[]", StringComparison.InvariantCulture) != -1)
                {
                    return EngineHandle.FromObjectGen(_documentIds.Select(LoadDocumentInternal).ToList());
                } // array

                if (_documentIds.Count == 0)
                    return EngineHandle.Null;

                return LoadDocumentInternal(_documentIds.First());
            }

            return CreateErrorAndSetLastExceptionIfNeeded(
                new InvalidOperationException("loadPath(doc, path) must be called with a valid document instance, but got a JS object instead"),
                JSValueType.ExecutionError);
        }
    }

    private T LoadDocumentInternal(string id)
    {
        if (string.IsNullOrEmpty(id))
        {
            return EngineHandle.Undefined;
        }

        Document document;
        try
        {
            document = _database.DocumentsStorage.Get(_docsCtx, id);
        }
        catch (Exception e)
        {
            return CreateErrorAndSetLastExceptionIfNeeded(e, JSValueType.ExecutionError);
        }
        
        if (DebugMode)
        {
            DebugActions.LoadDocument.Add(new DynamicJsonValue { ["Id"] = id, ["Exists"] = document != null });
        }

        return JsUtils.TranslateToJs(_jsonCtx, document, keepAlive: true);
    }

    public T DeleteDocument(T self, T[] args)
    {
        if (args.Length != 1 && args.Length != 2)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("delete(id, changeVector) must be called with at least one parameter"), JSValueType.ExecutionError);

        if (args[0].IsString == false)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("delete(id, changeVector) id argument must be a string"), JSValueType.ExecutionError);

        var id = args[0].AsString;
        string changeVector = null;

        if (args.Length == 2 && args[1].IsString)
            changeVector = args[1].AsString;

        PutOrDeleteCalled = true;
        if (AssertValidDatabaseContext(("delete document"), out var val) == false)
        {
            return val;
        }

        if (AssertNotReadOnly(out val) == false)
        {
            return val;
        }

        DocumentsStorage.DeleteOperationResult? result;

        try
        {
            result = _database.DocumentsStorage.Delete(_docsCtx, id, changeVector);
        }
        catch (Exception e)
        {
            return CreateErrorAndSetLastExceptionIfNeeded(e, JSValueType.ExecutionError);
        }

        if (RefreshOriginalDocument && string.Equals(OriginalDocumentId, id, StringComparison.OrdinalIgnoreCase))
            RefreshOriginalDocument = false;

        if (DebugMode)
        {
            DebugActions.DeleteDocument.Add(id);
        }

        return EngineHandle.CreateValue(result != null);
    }

    public T PutDocument(T self, T[] args)
    {
        string changeVector = null;

        if (args.Length != 2 && args.Length != 3)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("put(id, doc, changeVector) must be called with called with 2 or 3 arguments only"),
                JSValueType.ExecutionError);
        if (AssertValidDatabaseContext(("put document"), out var val) == false)
        {
            return val;
        }

        if (AssertNotReadOnly(out val) == false)
        {
            return val;
        }

        if (args[0].IsString == false && args[0].IsNull == false && args[0].IsUndefined == false)
            return AssertValidId();

        var id = args[0].IsNull || args[0].IsUndefined ? null : args[0].AsString;

        if (args[1].IsObject == false)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException(
                $"Created document must be a valid object which is not null or empty. Document ID: '{id}'."), JSValueType.ExecutionError);

        PutOrDeleteCalled = true;

        if (args.Length == 3)
            if (args[2].IsString)
                changeVector = args[2].AsString;
            else if (args[2].IsNull == false && args[0].IsUndefined == false)
                return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"The change vector must be a string or null. Document ID: '{id}'."), JSValueType.ExecutionError);

        BlittableJsonReaderObject reader = null;

        try
        {
            reader = JsBlittableBridge.Translate(_jsonCtx, args[1], usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);

            DocumentsStorage.PutOperationResults put;
            try
            {
                put= _database.DocumentsStorage.Put(
                    _docsCtx,
                    id,
                    _docsCtx.GetLazyString(changeVector),
                    reader,
                    //RavenDB-11391 Those flags were added to cause attachment/counter metadata table check & remove metadata properties if not necessary
                    nonPersistentFlags: NonPersistentDocumentFlags.ResolveAttachmentsConflict | NonPersistentDocumentFlags.ResolveCountersConflict | NonPersistentDocumentFlags.ResolveTimeSeriesConflict
                );

            }
            catch (Exception e)
            {
                return CreateErrorAndSetLastExceptionIfNeeded(e, JSValueType.ExecutionError);
            }
            
            if (DebugMode)
            {
                DebugActions.PutDocument.Add(new DynamicJsonValue
                {
                    ["Id"] = put.Id,
                    ["Data"] = reader
                });
            }

            if (RefreshOriginalDocument == false && string.Equals(put.Id, OriginalDocumentId, StringComparison.OrdinalIgnoreCase))
                RefreshOriginalDocument = true;

            return EngineHandle.CreateValue(put.Id);
        }
        finally
        {
            if (DebugMode == false)
                reader?.Dispose();
        }
    }

    public T CompareExchange(T self, T[] args)
    {
        if (AssertValidDatabaseContext(("cmpxchg"), out var val) == false)
        {
            return val;
        }

        if (args.Length != 1 && args.Length != 2 || args[0].IsStringEx == false)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("cmpxchg(key) must be called with a single string argument"), JSValueType.ExecutionError);

        return CmpXchangeInternal(CompareExchangeKey.GetStorageKey(_database.Name, args[0].AsString));
    }

    private T CmpXchangeInternal(string key)
    {
        if (string.IsNullOrEmpty(key))
            return EngineHandle.Undefined;

        using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            var value = _database.ServerStore.Cluster.GetCompareExchangeValue(ctx, key).Value;
            if (value == null)
                return EngineHandle.Null;

            var jsValue = JsUtils.TranslateToJs(_jsonCtx, value.Clone(_jsonCtx));
            //TODO: egor was: return jsValue.AsObject().Get(Constants.CompareExchange.ObjectFieldName);
            return jsValue.GetProperty(Constants.CompareExchange.ObjectFieldName);
        }
    }

    public T GetCounter(T self, T[] args)
    {
        return GetCounterInternal(args);

    }

    public T GetCounterRaw(T self, T[] args)
    {
        return GetCounterInternal(args, true);

    }

    private T GetCounterInternal(T[] args, bool raw = false)
    {
        var signature = raw ? "counterRaw(doc, name)" : "counter(doc, name)";
        if (AssertValidDatabaseContext((signature), out var val) == false)
        {
            return val;
        }

        if (args.Length != 2)
           return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"{signature} must be called with exactly 2 arguments"), JSValueType.ExecutionError);

        string id;
        if (args[0].IsObject && args[0].AsObject() is IBlittableObjectInstance<T> doc)
        {
            id = doc.DocumentId;
        }
        else if (args[0].IsStringEx)
        {
            id = args[0].AsString;
        }
        else
        {
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself"), JSValueType.ExecutionError);
        }

        if (args[1].IsStringEx == false)
        {
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"{signature}: 'name' must be a string argument"), JSValueType.ExecutionError);
        }

        var name = args[1].AsString;
        if (string.IsNullOrEmpty(id) || string.IsNullOrEmpty(name))
        {
            return EngineHandle.Undefined;
        }

        if (raw == false)
        {
            T counterValue;
            bool exists;
            var counterValue1 = _database.DocumentsStorage.CountersStorage.GetCounterValue(_docsCtx, id, name);
            if (counterValue1.HasValue)
            {
                var val123 = counterValue1.Value;
                var actualValue = val123.Value;

                counterValue = EngineHandle.CreateValue(actualValue);
                exists = true;
            }
            else
            {
                counterValue = EngineHandle.Null;
                exists = false;

            }

            if (DebugMode)
            {
                DebugActions.GetCounter.Add(new DynamicJsonValue
                {
                    ["Name"] = name,
                    ["Value"] = counterValue.ToString(),
                    ["Exists"] = exists
                });
            }

            return counterValue;
        }

        var obj = EngineHandle.CreateObject();
        foreach (var partialValue in _database.DocumentsStorage.CountersStorage.GetCounterPartialValues(_docsCtx, id, name))
        {
            obj.FastAddProperty(partialValue.ChangeVector, EngineHandle.CreateValue(partialValue.PartialValue), writable: true, enumerable: false, configurable: false);
        }

        return obj;
    }

    public T IncrementCounter(T self, T[] args)
    {
        if (AssertValidDatabaseContext(("incrementCounter"), out var val) == false)
        {
            return val;
        }

        if (args.Length < 2 || args.Length > 3)
        {
            return ThrowInvalidIncrementCounterArgs(args);
        }

        var signature = args.Length == 2 ? "incrementCounter(doc, name)" : "incrementCounter(doc, name, value)";

        BlittableJsonReaderObject docBlittable = null;
        string id = null;

        if (args[0].IsObject && args[0].AsObject() is IBlittableObjectInstance<T> doc)
        {
            id = doc.DocumentId;
            docBlittable = doc.Blittable;
        }
        else if (args[0].IsStringEx)
        {
            id = args[0].AsString;
            var document = _database.DocumentsStorage.Get(_docsCtx, id);
            if (document == null)
            {
               return ThrowMissingDocument(id);
                Debug.Assert(false); // never hit
            }

            docBlittable = document.Data;
        }
        else
        {
            return ThrowInvalidDocumentArgsType(signature);
        }

        Debug.Assert(id != null && docBlittable != null);

        if (args[1].IsStringEx == false)
            return ThrowInvalidCounterName(signature);

        var name = args[1].AsString;
        if (string.IsNullOrWhiteSpace(name))
            return ThrowInvalidCounterName(signature);

        double value = 1;
        if (args.Length == 3)
        {
            if (args[2].IsNumber == false)
                return ThrowInvalidCounterValue();
            value = args[2].AsDouble;
        }

        long? currentValue = null;
        if (DebugMode)
        {
            currentValue = _database.DocumentsStorage.CountersStorage.GetCounterValue(_docsCtx, id, name)?.Value;
        }

        _database.DocumentsStorage.CountersStorage.IncrementCounter(_docsCtx, id, CollectionName.GetCollectionName(docBlittable), name, (long)value, out var exists);

        if (exists == false)
        {
            DocumentCountersToUpdate ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            DocumentCountersToUpdate.Add(id);
        }

        if (DebugMode)
        {
            var newValue = _database.DocumentsStorage.CountersStorage.GetCounterValue(_docsCtx, id, name)?.Value;

            DebugActions.IncrementCounter.Add(new DynamicJsonValue
            {
                ["Name"] = name,
                ["OldValue"] = currentValue,
                ["AddedValue"] = value,
                ["NewValue"] = newValue,
                ["Created"] = exists == false
            });
        }

        return EngineHandle.True;
    }

    public T DeleteCounter(T self, T[] args)
    {
        if (AssertValidDatabaseContext(("deleteCounter"), out var val) == false)
        {
            return val;
        }

        if (args.Length != 2)
        {
            return ThrowInvalidDeleteCounterArgs();
        }

        string id = null;
        BlittableJsonReaderObject docBlittable = null;

        //args[0].TryGetDocumentIdAndBlittable
        if (args[0].IsObject && args[0].AsObject() is IBlittableObjectInstance<T> doc)
        {
            id = doc.DocumentId;
            docBlittable = doc.Blittable;
        }
        else if (args[0].IsStringEx)
        {
            id = args[0].AsString;
            var document = _database.DocumentsStorage.Get(_docsCtx, id);
            if (document == null)
            {
                return ThrowMissingDocument(id);
                Debug.Assert(false); // never hit
            }

            docBlittable = document.Data;
        }
        else
        {
            return ThrowInvalidDeleteCounterDocumentArg();
        }

        Debug.Assert(id != null && docBlittable != null);

        if (args[1].IsStringEx == false)
        {
            return ThrowDeleteCounterNameArg();
        }

        var name = args[1].AsString;
        _database.DocumentsStorage.CountersStorage.DeleteCounter(_docsCtx, id, CollectionName.GetCollectionName(docBlittable), name);

        DocumentCountersToUpdate ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        DocumentCountersToUpdate.Add(id);

        if (DebugMode)
        {
            DebugActions.DeleteCounter.Add(name);
        }

        return EngineHandle.True;
    }

    public T GetLastModified(T self, T[] args)
    {
        if (args.Length != 1)
           return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("lastModified(doc) must be called with a single argument"), JSValueType.ExecutionError);

        if (args[0].IsNull || args[0].IsUndefined)
            return args[0];

        if (args[0].IsObject == false)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("lastModified(doc) must be called with an object argument"), JSValueType.ExecutionError);

        if (args[0].AsObject() is IBlittableObjectInstance<T> doc)
        {
            if (doc.LastModified == null)
                return EngineHandle.Undefined;

            // we use UTC because last modified is in UTC
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var jsTime = doc.LastModified.Value.Subtract(epoch).TotalMilliseconds;
            return EngineHandle.CreateValue(jsTime);
        }

        return EngineHandle.Undefined;
    }

    public T StartsWith(T self, T[] args)
    {
        if (args.Length != 2 || args[0].IsStringEx == false || args[1].IsStringEx == false)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("startsWith(text, contained) must be called with two string parameters"), JSValueType.ExecutionError);

        return EngineHandle.CreateValue(args[0].AsString.StartsWith(args[1].AsString, StringComparison.OrdinalIgnoreCase));
    }

    public T EndsWith(T self, T[] args)
    {
        if (args.Length != 2 || args[0].IsStringEx == false || args[1].IsStringEx == false)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("endsWith(text, contained) must be called with two string parameters"), JSValueType.ExecutionError);

        return EngineHandle.CreateValue(args[0].AsString.EndsWith(args[1].AsString, StringComparison.OrdinalIgnoreCase));
    }

    public T Regex(T self, T[] args)
    {
        if (args.Length != 2 || args[0].IsStringEx == false || args[1].IsStringEx == false)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("regex(text, regex) must be called with two string parameters"), JSValueType.ExecutionError);

        var regex = _regexCache.Get(args[1].AsString);

        return regex.IsMatch(args[0].AsString) ? EngineHandle.True : EngineHandle.False;
    }

    public T ConvertJsTimeToTimeSpanString(T self, T[] args)
    {
        if (args.Length != 1 || args[0].IsNumber == false)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("convertJsTimeToTimeSpanString(ticks) must be called with a single long argument"), JSValueType.ExecutionError);

        var ticks = Convert.ToInt64(args[0].AsDouble) * 10000;

        var asTimeSpan = new TimeSpan(ticks);

        return EngineHandle.CreateValue(asTimeSpan.ToString());
    }

    public T ConvertToTimeSpanString(T self, T[] args)
    {
        if (args.Length == 1)
        {
            if (args[0].IsNumber == false)
                return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("convertToTimeSpanString(ticks) must be called with a single long argument"), JSValueType.ExecutionError);

            var ticks = Convert.ToInt64(args[0].AsDouble);
            var asTimeSpan = new TimeSpan(ticks);
            return EngineHandle.CreateValue(asTimeSpan.ToString());
        }

        if (args.Length == 3)
        {
            if (args[0].IsNumber == false || args[1].IsNumber == false || args[2].IsNumber == false)
                return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("convertToTimeSpanString(hours, minutes, seconds) must be called with integer values"), JSValueType.ExecutionError);

            var hours = Convert.ToInt32(args[0].AsInt32);
            var minutes = Convert.ToInt32(args[1].AsInt32);
            var seconds = Convert.ToInt32(args[2].AsInt32);

            var asTimeSpan = new TimeSpan(hours, minutes, seconds);
            return EngineHandle.CreateValue(asTimeSpan.ToString());
        }

        if (args.Length == 4)
        {
            if (args[0].IsNumber == false || args[1].IsNumber == false || args[2].IsNumber == false || args[3].IsNumber == false)
                return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("convertToTimeSpanString(days, hours, minutes, seconds) must be called with integer values"), JSValueType.ExecutionError);

            var days = Convert.ToInt32(args[0].AsInt32);
            var hours = Convert.ToInt32(args[1].AsInt32);
            var minutes = Convert.ToInt32(args[2].AsInt32);
            var seconds = Convert.ToInt32(args[3].AsInt32);

            var asTimeSpan = new TimeSpan(days, hours, minutes, seconds);
            return EngineHandle.CreateValue(asTimeSpan.ToString());
        }

        if (args.Length == 5)
        {
            if (args[0].IsNumber == false || args[1].IsNumber == false || args[2].IsNumber == false || args[3].IsNumber == false || args[4].IsNumber == false)
                return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("convertToTimeSpanString(days, hours, minutes, seconds, milliseconds) must be called with integer values"), JSValueType.ExecutionError);

            var days = Convert.ToInt32(args[0].AsInt32);
            var hours = Convert.ToInt32(args[1].AsInt32);
            var minutes = Convert.ToInt32(args[2].AsInt32);
            var seconds = Convert.ToInt32(args[3].AsInt32);
            var milliseconds = Convert.ToInt32(args[4].AsInt32);

            var asTimeSpan = new TimeSpan(days, hours, minutes, seconds, milliseconds);
            return EngineHandle.CreateValue(asTimeSpan.ToString());
        }

        return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("supported overloads are: " +
                                            "convertToTimeSpanString(ticks), " +
                                            "convertToTimeSpanString(hours, minutes, seconds), " +
                                            "convertToTimeSpanString(days, hours, minutes, seconds), " +
                                            "convertToTimeSpanString(days, hours, minutes, seconds, milliseconds)"), JSValueType.ExecutionError);
    }

    public T CompareDates(T self, T[] args)
    {
        if (args.Length < 1 || args.Length > 3)
        {
           return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"No overload for method 'compareDates' takes {args.Length} arguments. " +
                                                "Supported overloads are : compareDates(date1, date2), compareDates(date1, date2, operationType)"), JSValueType.ExecutionError);
        }

        ExpressionType binaryOperationType;
        if (args.Length == 2)
        {
            binaryOperationType = ExpressionType.Subtract;
        }
        else if (args[2].IsStringEx == false ||
                 Enum.TryParse(args[2].AsString, out binaryOperationType) == false)
        {
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("compareDates(date1, date2, operationType) : 'operationType' must be a string argument representing a valid 'ExpressionType'"), JSValueType.ExecutionError);
        }

        dynamic date1, date2;
        if ((binaryOperationType == ExpressionType.Equal ||
             binaryOperationType == ExpressionType.NotEqual) &&
            args[0].IsStringEx && args[1].IsStringEx)
        {
            date1 = args[0].AsString;
            date2 = args[1].AsString;
        }
        else
        {
            const string signature = "compareDates(date1, date2, binaryOp)";

            if (GetDateArg(args[0], signature, "date1", out var dt1) == false)
            {
                return CreateErrorAndSetLastExceptionIfNeeded(new ArgumentException($"{signature} : date1 must be of type 'DateInstance' or a DateTime string. {GetTypes(args[0])}"),
                    JSValueType.ExecutionError);
            }

            if (GetDateArg(args[1], signature, "date2", out var dt2) == false)
            {
                return CreateErrorAndSetLastExceptionIfNeeded(new ArgumentException($"{signature} : date2 must be of type 'DateInstance' or a DateTime string. {GetTypes(args[1])}"),
                    JSValueType.ExecutionError);
            }

            date1 = dt1;
            date2 = dt2;
        }

        switch (binaryOperationType)
        {
            case ExpressionType.Subtract:
                return EngineHandle.CreateValue((date1 - date2).ToString());
            case ExpressionType.GreaterThan:
                return date1 > date2 ? EngineHandle.True : EngineHandle.False;
            case ExpressionType.GreaterThanOrEqual:
                return date1 >= date2 ? EngineHandle.True : EngineHandle.False;
            case ExpressionType.LessThan:
                return date1 < date2 ? EngineHandle.True : EngineHandle.False;
            case ExpressionType.LessThanOrEqual:
                return date1 <= date2 ? EngineHandle.True : EngineHandle.False;
            case ExpressionType.Equal:
                return date1 == date2 ? EngineHandle.True : EngineHandle.False;
            case ExpressionType.NotEqual:
                return date1 != date2 ? EngineHandle.True : EngineHandle.False;
            default:
                return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"compareDates(date1, date2, binaryOp) : unsupported binary operation '{binaryOperationType}'"), JSValueType.ExecutionError);
        }
    }

    public unsafe bool GetDateArg(T arg, string signature, string argName, out DateTime val)
    {
        if (arg.IsDate)
        {
            val = arg.AsDate;
            return true;
        }

        if (arg.IsStringEx == false)
        {
            val = default;
            return false;
        }

        var s = arg.AsString;
        fixed (char* pValue = s)
        {
            var result = LazyStringParser.TryParseDateTime(pValue, s.Length, out DateTime dt, out _, properlyParseThreeDigitsMilliseconds: true);
            if (result != LazyStringParser.Result.DateTime)
            {
                val = default;
                return false;
            }
                  
            val = dt;
            return true;
        }
    }

    public unsafe T ToStringWithFormat(T self, T[] args)
    {
        if (args.Length < 1 || args.Length > 3)
        {
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"No overload for method 'toStringWithFormat' takes {args.Length} arguments. " +
                                                                          "Supported overloads are : toStringWithFormat(object), toStringWithFormat(object, format), toStringWithFormat(object, culture), toStringWithFormat(object, format, culture)."), JSValueType.ExecutionError);
        }

        var cultureInfo = CultureInfo.InvariantCulture;
        string format = null;

        for (var i = 1; i < args.Length; i++)
        {
            if (args[i].IsStringEx == false)
            {
                return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("toStringWithFormat : 'format' and 'culture' must be string arguments"), JSValueType.ExecutionError);
            }

            var arg = args[i].AsString;
            if (CultureHelper.Cultures.TryGetValue(arg, out var culture))
            {
                cultureInfo = culture;
                continue;
            }

            format = arg;
        }

        if (args[0].IsDate)
        {
            var date = args[0].AsDate;
            return format != null ?
                EngineHandle.CreateValue(date.ToString(format, cultureInfo)) :
                EngineHandle.CreateValue(date.ToString(cultureInfo));
        }

        if (args[0].IsNumberOrIntEx)
        {
            var num = args[0].AsDouble;
            return format != null ?
                EngineHandle.CreateValue(num.ToString(format, cultureInfo)) :
                EngineHandle.CreateValue(num.ToString(cultureInfo));
        }

        if (args[0].IsStringEx)
        {
            var s = args[0].AsString;
            fixed (char* pValue = s)
            {
                var result = LazyStringParser.TryParseDateTime(pValue, s.Length, out DateTime dt, out _, properlyParseThreeDigitsMilliseconds: true);
                switch (result)
                {
                    case LazyStringParser.Result.DateTime:
                        return format != null ?
                            EngineHandle.CreateValue(dt.ToString(format, cultureInfo)) :
                            EngineHandle.CreateValue(dt.ToString(cultureInfo));
                    default:
                       return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("toStringWithFormat(dateString) : 'dateString' is not a valid DateTime string"), JSValueType.ExecutionError);
                }
            }
        }

        if (args[0].IsBoolean == false)
        {
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"toStringWithFormat() is not supported for objects of type {args[0].ValueType} "),
                JSValueType.ExecutionError);
        }

        var boolean = args[0].AsBoolean;
        return EngineHandle.CreateValue(boolean.ToString(cultureInfo));
    }

    protected abstract bool TryGetLambdaPropertyName(T param, out string propName);
    protected abstract bool ScalarToRawStringInternal(T param);
    public T ScalarToRawString(T self, T[] args)
    {
        if (args.Length != 2)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("scalarToRawString(document, lambdaToField) may be called on with two parameters only"),
                JSValueType.ExecutionError);

        T firstParam = args[0];
        if (firstParam.IsObject && args[0].AsObject() is IBlittableObjectInstance<T> selfInstance)
        {

            if (TryGetLambdaPropertyName(args[1], out var propName))
            {
                if (selfInstance.TryGetValue(propName, out IBlittableObjectProperty<T> existingValue, out _) && existingValue != null)
                {
                    if (existingValue.Changed)
                    {
                        return existingValue.ValueHandle;
                    }
                }

                var propertyIndex = selfInstance.Blittable.GetPropertyIndex(propName);

                if (propertyIndex == -1)
                {
                    return EngineHandle.CreateObject();
                }

                BlittableJsonReaderObject.PropertyDetails propDetails = new BlittableJsonReaderObject.PropertyDetails();
                selfInstance.Blittable.GetPropertyByIndex(propertyIndex, ref propDetails);
                var value = propDetails.Value;
                BlittableJsonToken type = propDetails.Token & BlittableJsonReaderBase.TypesMask;
                switch (type)
                {
                    case BlittableJsonToken.Null:
                        return EngineHandle.Null;
                    case BlittableJsonToken.Boolean:
                        return (bool)value ? EngineHandle.True : EngineHandle.False;
                    case BlittableJsonToken.Integer:
                        switch (value)
                        {
                            case int intValue:
                                return EngineHandle.CreateValue(intValue);
                            case long:
                                return CreateObjectBinder(type, value);
                            default:
                                return CreateErrorAndSetLastExceptionIfNeeded(new ArgumentException($"Invalid value type, expected int or long but got '{value.GetType().FullName}'"), JSValueType.ExecutionError);
                        }
                    default:
                        return CreateObjectBinder(type, value);
                }
            }

            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("scalarToRawString(document, lambdaToField) must be called with a second lambda argument"), JSValueType.ExecutionError);
        }

        return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("scalarToRawString(document, lambdaToField) may be called with a document first parameter only"), JSValueType.ExecutionError);
    }

    public abstract void CleanInternal();
    protected abstract T CreateObjectBinder(BlittableJsonToken type, object value);

    public T OutputDebug(T self, T[] args)
    {
        if (DebugMode == false)
            return self;

        var obj = args[0];

        DebugOutput.Add(GetDebugValue(obj, recursive: false));
        return self;
    }

    private string GetDebugValue(T obj, bool recursive)
    {
        if (obj.IsStringEx)
        {
            var debugValue = obj.AsString;
            return recursive ? '"' + debugValue + '"' : debugValue;
        }
        if (obj.IsArray)
        {
            var sb = new StringBuilder("[");
            int arrayLength = obj.ArrayLength;
            for (int i = 0; i < arrayLength; i++)
            {
                if (i != 0)
                    sb.Append(",");
                using (var jsValue = obj.GetProperty(i))
                    sb.Append(GetDebugValue(jsValue, recursive: true));
            }
            sb.Append("]");
            return sb.ToString();
        }
        if (obj.IsObject)
        {
            //TODO: egor in jint it was: if (obj is BlittableObjectInstanceJint boi && boi.Changed == false)
            if (obj.AsObject() is IBlittableObjectInstance<T> boi && boi.Changed == false)
            {
                return boi.Blittable.ToString();
            }

            using (var blittable = JsBlittableBridge.Translate(_jsonCtx, obj, isRoot: !recursive))
            {
                return blittable.ToString();
            }
        }
        if (obj.IsBoolean)
            return obj.AsBoolean.ToString();
        if (obj.IsInt32)
            return obj.AsInt32.ToString(CultureInfo.InvariantCulture);
        if (obj.IsNumberEx)
            return obj.AsDouble.ToString(CultureInfo.InvariantCulture);
        if (obj.IsNull)
            return "null";
        if (obj.IsUndefined)
            return "undefined";
        return obj.ToString();
    }

    public T Spatial_Distance(T self, T[] args)
    {
        if (args.Length < 4 && args.Length > 5)
            return CreateErrorAndSetLastExceptionIfNeeded(new ArgumentException("Called with expected number of arguments, expected: spatial.distance(lat1, lng1, lat2, lng2, kilometers | miles | cartesian)"), JSValueType.ExecutionError);

        for (int i = 0; i < 4; i++)
        {
            if (args[i].IsNumber == false)
                return EngineHandle.Undefined;
        }

        var lat1 = args[0].AsDouble;
        var lng1 = args[1].AsDouble;
        var lat2 = args[2].AsDouble;
        var lng2 = args[3].AsDouble;

        var units = SpatialUnits.Kilometers;
        if (args.Length > 4 && args[4].IsStringEx)
        {
            if (string.Equals("cartesian", args[4].AsString, StringComparison.OrdinalIgnoreCase))
                return EngineHandle.CreateValue(SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator.CartesianDistance(lat1, lng1, lat2, lng2));

            if (Enum.TryParse(args[4].AsString, ignoreCase: true, out units) == false)
                return CreateErrorAndSetLastExceptionIfNeeded(new ArgumentException("Unable to parse units " + args[5] + ", expected: 'kilometers' or 'miles'"), JSValueType.ExecutionError);
        }

        var result = SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator.HaverstineDistanceInMiles(lat1, lng1, lat2, lng2);
        if (units == SpatialUnits.Kilometers)
            result *= DistanceUtils.MILES_TO_KM;

        return EngineHandle.CreateValue(result);
    }

    public T IncludeDoc(T self, T[] args)
    {
        if (args.Length != 1)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("include(id) must be called with a single argument"), JSValueType.ExecutionError);

        if (args[0].IsNull || args[0].IsUndefined)
            return args[0];

        if (args[0].IsArray)// recursive call ourselves
        {
            var array = args[0];

            //TODO: egor why it is ++1? in the methods below as well
            for (int i = 0; i < array.ArrayLength; ++i)
            {
                // TODO: egor this is copy paste from v8 I need to handle arrays (arrayinstance) and go over "length"
                using (var jsItem = array.GetProperty(i))
                {
                    args[0].Set(jsItem);
                    if (args[0].IsStringEx)
                        IncludeDoc(self, args);
                }
            }

            return self;
        }

        if (args[0].IsStringEx == false)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("include(doc) must be called with an string or string array argument"), JSValueType.ExecutionError);

        var id = args[0].AsString;

        if (Includes == null)
            Includes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        Includes.Add(id);

        return self;
    }

    public T IncludeCompareExchangeValue(T self, T[] args)
    {
        if (args.Length != 1)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("includes.cmpxchg(key) must be called with a single argument"), JSValueType.ExecutionError);

        if (args[0].IsNull || args[0].IsUndefined)
            return self;

        if (args[0].IsArray)// recursive call ourselves
        {
            var jsArray = args[0];
            // TODO: egor this is copy paste from v8 I need to handle arrays (arrayinstance) and go over "length" (same as above method)
            int arrayLength = jsArray.ArrayLength;
            for (int i = 0; i < arrayLength; ++i)
            {
                using (args[0] = jsArray.GetProperty(i))
                {
                    if (args[0].IsStringEx)
                        IncludeCompareExchangeValue(self, args);
                }
            }

            return self;
        }

        if (args[0].IsStringEx == false)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("includes.cmpxchg(key) must be called with an string or string array argument"), JSValueType.ExecutionError);

        var key = args[0].AsString;

        if (CompareExchangeValueIncludes == null)
            CompareExchangeValueIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        CompareExchangeValueIncludes.Add(key);

        return self;
    }

    public T IncludeRevisions(T self, T[] args)
    {
        if (args == null)
            return EngineHandle.Null;

        IncludeRevisionsChangeVectors ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (T arg in args)
        {
            //TODO: egor here we use v8 value types, we might want to use our own?
            switch (arg.ValueType)
            {
                case JSValueType.String:
                    if (DateTime.TryParseExact(arg.ToString(), DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal, out var dateTime))
                    {
                        IncludeRevisionByDateTimeBefore = dateTime.ToUniversalTime();
                        continue;
                    }
                    IncludeRevisionsChangeVectors.Add(arg.ToString());
                    break;
                case JSValueType.Object when arg.IsArray:
                    T jsArray = arg;
                    int arrayLength = jsArray.ArrayLength;
                    for (int i = 0; i < arrayLength; ++i)
                    {
                        using (var jsItem = jsArray.GetProperty(i))
                        {
                            if (jsItem.IsStringEx == false)
                                continue;
                            IncludeRevisionsChangeVectors.Add(jsItem.ToString());
                        }
                    }
                    break;
            }
        }

        return EngineHandle.Null;
    }

    public T InvokeTimeSeriesFunction(string name, T[] args)
    {
        if (AssertValidDatabaseContext(("InvokeTimeSeriesFunction"), out var val) == false)
        {
            return val;
        }

        if (_runner.TimeSeriesDeclaration.TryGetValue(name, out var func) == false)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"Failed to invoke time series function. Unknown time series name '{name}'."),
                JSValueType.ExecutionError);

        if (GetTimeSeriesFunctionArgs(name, args, out string docId, out var lazyIds, out var tsFunctionArgs, out var e) == false)
        {
            return e;
        }

        var queryParams = ((Document)tsFunctionArgs[^1]).Data;

        var retriever = new TimeSeriesRetriever(_docsCtx, queryParams, loadedDocuments: null, token: _token);

        var streamableResults = retriever.InvokeTimeSeriesFunction(func, docId, tsFunctionArgs, out var type);
        var result = retriever.MaterializeResults(streamableResults, type, addProjectionToResult: false, fromStudio: false);

        foreach (var id in lazyIds)
        {
            id?.Dispose();
        }

        return TranslateToJs(_jsonCtx, result, keepAlive: true);
    }

    public T TimeSeries(T self, T[] args)
    {
        if (AssertValidDatabaseContext((_timeSeriesSignature), out var val) == false)
        {
            return val;
        }

        if (args.Length != 2)
            return CreateErrorAndSetLastExceptionIfNeeded(new ArgumentException($"{_timeSeriesSignature}: This method requires 2 arguments but was called with {args.Length}"), JSValueType.ExecutionError);

        var obj = EngineHandle.CreateObject();
        obj.SetProperty("append", EngineHandle.CreateClrCallBack("append", AppendTimeSeries));
        obj.SetProperty("increment", EngineHandle.CreateClrCallBack("increment", IncrementTimeSeries));
        obj.SetProperty("delete", EngineHandle.CreateClrCallBack("delete", DeleteRangeTimeSeries));
        obj.SetProperty("get", EngineHandle.CreateClrCallBack("get", GetRangeTimeSeries));
        obj.SetProperty("getStats", EngineHandle.CreateClrCallBack("getStats", GetStatsTimeSeries));
        obj.SetProperty("doc", args[0]);
        obj.SetProperty("name", args[1]);
        return obj;
    }

    private T GetRangeTimeSeries(T self, T[] args)
    {
        using var document = self.GetProperty("doc");
        using var name = self.GetProperty("name");
        if (AssertValidDatabaseContext(("get"), out var val) == false)
        {
            return val;
        }

        const string getRangeSignature = "get(from, to)";
        const string getAllSignature = "get()";

        if (GetIdFromArg(document, _timeSeriesSignature, out var id) == false)
        {
            return CreateErrorAndSetLastExceptionIfNeeded(
                new InvalidOperationException(
                    $"{_timeSeriesSignature}: 'doc' must be a string argument (the document id) or the actual document instance itself. {GetTypes(document)}"),
                JSValueType.ExecutionError);
        }

        if (GetStringArg(name, _timeSeriesSignature, "name", out var timeSeries, out var e) == false)
        {
            return e;
        }

        DateTime from, to;
        switch (args.Length)
        {
            case 0:
                from = DateTime.MinValue;
                to = DateTime.MaxValue;
                break;
            case 2:
                if (GetTimeSeriesDateArg(args[0], getRangeSignature, "from", out from, out var exception) == false)
                {
                    return CreateErrorAndSetLastExceptionIfNeeded(
                        exception ?? new ArgumentException($"{getRangeSignature} : 'from' must be of type 'DateInstance' or a DateTime string. {GetTypes(args[0])}"),
                        JSValueType.ExecutionError);
                }

                if (GetTimeSeriesDateArg(args[1], getRangeSignature, "to", out to, out exception) == false)
                {
                    return CreateErrorAndSetLastExceptionIfNeeded(exception ??
                                                    new ArgumentException(
                                                        $"{getRangeSignature} : 'to' must be of type 'DateInstance' or a DateTime string. {GetTypes(args[1])}"),
                        JSValueType.ExecutionError);
                }

                break;
            default:
                return CreateErrorAndSetLastExceptionIfNeeded(
                    new ArgumentException(
                        $"'get' method has only the overloads: '{getRangeSignature}' or '{getAllSignature}', but was called with {args.Length} arguments."),
                    JSValueType.ExecutionError);
        }

        TimeSeriesReader reader;
        try
        {
            reader = _database.DocumentsStorage.TimeSeriesStorage.GetReader(_docsCtx, id, timeSeries, from, to);
        }
        catch (Exception exception)
        {
            return CreateErrorAndSetLastExceptionIfNeeded(exception, JSValueType.ExecutionError);
        }

        var entries = new List<T>();
        foreach (var singleResult in reader.AllValues())
        {
            Span<double> valuesSpan = singleResult.Values.Span;
            var jsSpanItems = new T[valuesSpan.Length];
            for (int i = 0; i < valuesSpan.Length; i++)
            {
                jsSpanItems[i] = EngineHandle.CreateValue(valuesSpan[i]);
            }

            var entry = EngineHandle.CreateObject();
            entry.SetProperty(nameof(TimeSeriesEntry.Timestamp), EngineHandle.CreateValue(singleResult.Timestamp.GetDefaultRavenFormat(isUtc: true)));
            entry.SetProperty(nameof(TimeSeriesEntry.Tag), singleResult.Tag == null ? EngineHandle.Null : EngineHandle.CreateValue(singleResult.Tag.ToString()));
            entry.SetProperty(nameof(TimeSeriesEntry.Values), EngineHandle.CreateArray(jsSpanItems));
            entry.SetProperty(nameof(TimeSeriesEntry.IsRollup), EngineHandle.CreateValue(singleResult.Type == SingleResultType.RolledUp));

            entries.Add(entry);

            if (DebugMode)
            {
                DebugActions.GetTimeSeries.Add(new DynamicJsonValue
                {
                    ["Name"] = timeSeries,
                    ["Timestamp"] = singleResult.Timestamp.GetDefaultRavenFormat(isUtc: true),
                    ["Tag"] = singleResult.Tag?.ToString(),
                    ["Values"] = singleResult.Values.ToArray().Cast<object>(),
                    ["Type"] = singleResult.Type,
                    ["Exists"] = true
                });
            }
        }

        if (DebugMode && entries.Count == 0)
        {
            DebugActions.GetTimeSeries.Add(new DynamicJsonValue
            {
                ["Name"] = timeSeries,
                ["Exists"] = false
            });
        }

        if (entries.Count == 0)
            return EngineHandle.CreateEmptyArray();

        return EngineHandle.CreateArray(entries);
    }

    private bool GetIdFromArg(T docArg, string signature, out string val)
    {
        if (docArg.IsObject && docArg.AsObject() is IBlittableObjectInstance<T> doc)
        {
            val= doc.DocumentId;
            return true;
        }

        if (docArg.IsStringEx == false)
        {
            val = null;
            return false;
        }

        val = docArg.AsString;
        return true;
    }

    private T DeleteRangeTimeSeries(T self, T[] args)
    {
        using var document = self.GetProperty("doc");
        using var name = self.GetProperty("name");
        if (AssertValidDatabaseContext(("timeseries(doc, name).delete"), out var val) == false)
        {
            return val;
        }

        const string deleteAll = "delete()";
        const string deleteSignature = "delete(from, to)";

        DateTime from, to;
        switch (args.Length)
        {
            case 0:
                from = DateTime.MinValue;
                to = DateTime.MaxValue;
                break;
            case 2:
                if (GetTimeSeriesDateArg(args[0], deleteSignature, "from", out from, out Exception exception) == false)
                {                    return CreateErrorAndSetLastExceptionIfNeeded(exception ??
                                                                     new ArgumentException($"{deleteSignature} : 'from' must be of type 'DateInstance' or a DateTime string. {GetTypes(args[1])}"),
                    JSValueType.ExecutionError);
                    
                }

                if (GetTimeSeriesDateArg(args[1], deleteSignature, "to", out to, out exception) == false)
                {
                    return CreateErrorAndSetLastExceptionIfNeeded(exception ??
                                                    new ArgumentException($"{deleteSignature} : 'to' must be of type 'DateInstance' or a DateTime string. {GetTypes(args[1])}"),
                        JSValueType.ExecutionError);
                }

                break;
            default:
                return CreateErrorAndSetLastExceptionIfNeeded(
                    new ArgumentException($"'delete' method has only the overloads: '{deleteSignature}' or '{deleteAll}', but was called with {args.Length} arguments."),
                    JSValueType.ExecutionError);
        }

        if (GetIdAndDocFromArg(document, _timeSeriesSignature, out var id, out var doc, out var e) == false)
        {
            return e;
        }

        if (GetStringArg(name, _timeSeriesSignature, "name", out var timeSeries, out e) == false)
        {
            return e;
        }

        var count = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_docsCtx, id, timeSeries).Count;
        
        if (count == 0)
            return EngineHandle.Undefined;

        var deletionRangeRequest = new TimeSeriesStorage.DeletionRangeRequest
        {
            DocumentId = id,
            Collection = CollectionName.GetCollectionName(doc),
            Name = timeSeries,
            From = from,
            To = to,
        };
        _database.DocumentsStorage.TimeSeriesStorage.DeleteTimestampRange(_docsCtx, deletionRangeRequest, updateMetadata: false);

        count = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_docsCtx, id, timeSeries).Count;
        if (count == 0)
        {
            DocumentTimeSeriesToUpdate ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            DocumentTimeSeriesToUpdate.Add(id);
        }

        if (DebugMode)
        {
            DebugActions.DeleteTimeSeries.Add(new DynamicJsonValue
            {
                ["Name"] = timeSeries,
                ["From"] = from,
                ["To"] = to
            });
        }
        return EngineHandle.Undefined;
    }

    private T IncrementTimeSeries(T self, T[] args)
    {
        using var document = self.GetProperty("doc");
        using var name = self.GetProperty("name");

        if (AssertValidDatabaseContext(("timeseries(doc, name).increment"), out var val) == false)
        {
            return val;
        }

        const string signature1Args = "timeseries(doc, name).increment(values)";
        const string signature2Args = "timeseries(doc, name).increment(timestamp, values)";

        string signature;
        DateTime timestamp;
        T valuesArg;

        switch (args.Length)
        {
            case 1:
                signature = signature1Args;
                timestamp = DateTime.UtcNow.EnsureMilliseconds();
                valuesArg = args[0];
                break;
            case 2:
                signature = signature2Args;
                if (GetTimeSeriesDateArg(args[0], signature, "timestamp", out timestamp, out Exception exception) == false)
                {
                    return CreateErrorAndSetLastExceptionIfNeeded(exception ??
                                                    new ArgumentException($"{signature} : 'timestamp' must be of type 'DateInstance' or a DateTime string. {GetTypes(args[1])}"),
                        JSValueType.ExecutionError);
                }
                valuesArg = args[1];
                break;
            default:
             return   CreateErrorAndSetLastExceptionIfNeeded(new ArgumentException(
                    $"There is no overload with {args.Length} arguments for this method should be {signature1Args} or {signature2Args}"), JSValueType.ExecutionError);
        }

        if (GetIdAndDocFromArg(document, _timeSeriesSignature, out var id, out var doc, out var e) == false)
        {
            return e;
        }

       if(GetStringArg(name, _timeSeriesSignature, "name", out var timeSeries, out e) == false)
        {
            return e;
        }

        double[] valuesBuffer = null;
        try
        {
            if (GetTimeSeriesValues(valuesArg, ref valuesBuffer, signature, out var values, out T exception) == false)
            {
                return exception;
            }

            var tss = _database.DocumentsStorage.TimeSeriesStorage;
            var newSeries = tss.Stats.GetStats(_docsCtx, id, timeSeries).Count == 0;

            if (newSeries)
            {
                DocumentTimeSeriesToUpdate ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                DocumentTimeSeriesToUpdate.Add(id);
            }

            var toIncrement = new TimeSeriesOperation.IncrementOperation { Values = valuesBuffer, ValuesLength = values.Length, Timestamp = timestamp };

            tss.IncrementTimestamp(
                _docsCtx,
                id,
                CollectionName.GetCollectionName(doc),
                timeSeries,
                new[] { toIncrement },
                AppendOptionsForScript);

            if (DebugMode)
            {
                DebugActions.IncrementTimeSeries.Add(new DynamicJsonValue
                {
                    ["Name"] = timeSeries,
                    ["Timestamp"] = timestamp,
                    ["Values"] = values.ToArray().Cast<object>(),
                    ["Created"] = newSeries
                });
            }
        }
        finally
        {
            if (valuesBuffer != null)
                ArrayPool<double>.Shared.Return(valuesBuffer);
        }

        return EngineHandle.Undefined;
    }

    private T AppendTimeSeries(T self, T[] args)
    {
        if (AssertValidDatabaseContext(("timeseries(doc, name).append"), out var e) == false)
        {
            return e;
        }

        const string signature2Args = "timeseries(doc, name).append(timestamp, values)";
        const string signature3Args = "timeseries(doc, name).append(timestamp, values, tag)";

        string signature;
        LazyStringValue lsTag = null;
        switch (args.Length)
        {
            case 2:
                signature = signature2Args;
                break;
            case 3:
                signature = signature3Args;
                var tagArgument = args.Last();
                if ( /*tagArgument != null && */tagArgument.IsNull == false && tagArgument.IsUndefined == false)
                {
                    if (GetStringArg(tagArgument, signature, "tag", out var tag, out e) == false)
                    {
                        return e;
                    }

                    lsTag = _jsonCtx.GetLazyString(tag);
                }

                break;
            default:
                return CreateErrorAndSetLastExceptionIfNeeded(
                    new ArgumentException($"There is no overload with {args.Length} arguments for this method should be {signature2Args} or {signature3Args}"),
                    JSValueType.ExecutionError);
        }

        using var document = self.GetProperty("doc");
        using var name = self.GetProperty("name");
        if (GetIdAndDocFromArg(document, _timeSeriesSignature, out var id, out var doc, out e) == false)
        {
            return e;
        }

        if (GetStringArg(name, _timeSeriesSignature, "name", out var timeSeries, out e) == false)
        {
            return e;
        }

        if (GetTimeSeriesDateArg(args[0], signature, "timestamp", out var timestamp, out Exception exception) == false)
        {
            return CreateErrorAndSetLastExceptionIfNeeded(exception ??
                                            new ArgumentException($"{signature} : 'timestamp' must be of type 'DateInstance' or a DateTime string. {GetTypes(args[1])}"),
                JSValueType.ExecutionError);
        }

        double[] valuesBuffer = null;
        try
        {
            var valuesArg = args[1];

            if (GetTimeSeriesValues(valuesArg, ref valuesBuffer, signature, out var values, out e) == false)
            {
                return e;
            }

            var tss = _database.DocumentsStorage.TimeSeriesStorage;
            var newSeries = tss.Stats.GetStats(_docsCtx, id, timeSeries).Count == 0;

            if (newSeries)
            {
                DocumentTimeSeriesToUpdate ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                DocumentTimeSeriesToUpdate.Add(id);
            }

            var toAppend = new SingleResult {Values = values, Tag = lsTag, Timestamp = timestamp, Status = TimeSeriesValuesSegment.Live};

            tss.AppendTimestamp(
                _docsCtx,
                id,
                CollectionName.GetCollectionName(doc),
                timeSeries,
                new[] {toAppend},
                AppendOptionsForScript);

            if (DebugMode)
            {
                DebugActions.AppendTimeSeries.Add(new DynamicJsonValue
                {
                    ["Name"] = timeSeries,
                    ["Timestamp"] = timestamp,
                    ["Tag"] = lsTag,
                    ["Values"] = values.ToArray().Cast<object>(),
                    ["Created"] = newSeries
                });
            }
        }
        finally
        {
            if (valuesBuffer != null)
                ArrayPool<double>.Shared.Return(valuesBuffer);
        }

        return EngineHandle.Undefined;
    }

    private static bool GetTimeSeriesDateArg(T arg, string signature, string argName, out DateTime val, out Exception innerException)
    {
        if (arg.IsDate)
        {
            val = arg.AsDate;
            innerException = null;
            return true;
        }

        if (arg.IsStringEx == false)
        {
            val = default;
            innerException = null;
            return false;
        }

        var str = arg.AsString;
        if (TimeSeriesRetriever.TryParseDateTime(str, out DateTime dt1) == false)
        {
            val = default;
            innerException = TimeSeriesRetriever.GetArgumentException(str);
            return false;
        }

        val = dt1;
        innerException = null;
        return true;
    }

    private bool GetTimeSeriesValues(T valuesArg, ref double[] valuesBuffer, string signature, out Memory<double> values, out T exception)
    {
        if (valuesArg.IsArray)
        {
            valuesBuffer = ArrayPool<double>.Shared.Rent((int)valuesArg.ArrayLength);
            int arrayLength = valuesArg.ArrayLength;
            for (int i = 0; i < arrayLength; ++i)
            {
                using (var jsItem = valuesArg.GetProperty(i))
                {
                    if (jsItem.IsNumberOrIntEx == false)
                    {
                        values = null;
                        exception = CreateErrorAndSetLastExceptionIfNeeded(
                            new ArgumentException($"{signature}: The values argument must be an array of numbers, but got {jsItem.ValueType} key({i}) value({jsItem})"),
                            JSValueType.ExecutionError);
                        return false;
                    }

                    valuesBuffer[i] = jsItem.AsDouble;
                }
            }

            values = new Memory<double>(valuesBuffer, 0, (int)valuesArg.ArrayLength);
            exception = default;
            return true;
        }

        if (valuesArg.IsNumberOrIntEx)
        {
            valuesBuffer = ArrayPool<double>.Shared.Rent(1);
            valuesBuffer[0] = valuesArg.AsDouble;
            values = new Memory<double>(valuesBuffer, 0, 1);
            exception = default;
            return true;
        }

        values = null;
        exception = CreateErrorAndSetLastExceptionIfNeeded(new ArgumentException($"{signature}: The values should be an array but got {GetTypes(valuesArg)}"),
            JSValueType.ExecutionError);
        return false;
    }

    private T GetStatsTimeSeries(T self, T[] args)
    {
        using var document = self.GetProperty("doc");
        using var name = self.GetProperty("name");
        if (GetIdAndDocFromArg(document, _timeSeriesSignature, out var id, out _, out var e) == false)
        {
            return e;
        }

        if (GetStringArg(name, _timeSeriesSignature, "name", out var timeSeries, out e) == false)
        {
            return e;
        }

        (long Count, DateTime Start, DateTime End) stats;
        try
        {
            stats = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_docsCtx, id, timeSeries);
        }
        catch (Exception exception)
        {
            return CreateErrorAndSetLastExceptionIfNeeded(exception, JSValueType.ExecutionError);
        }

        var tsStats = EngineHandle.CreateObject();
        tsStats.SetProperty(nameof(stats.Start), EngineHandle.CreateValue(stats.Start));
        tsStats.SetProperty(nameof(stats.End), EngineHandle.CreateValue(stats.End));
        tsStats.SetProperty(nameof(stats.Count), EngineHandle.CreateValue(stats.Count));
        return tsStats;
    }

    private bool GetStringArg(T jsArg, string signature, string argName, out string val, out T exception)
    {
        if (jsArg.IsStringEx == false)
        {
            val = null;
            exception = CreateErrorAndSetLastExceptionIfNeeded(new ArgumentException($"{signature}: The '{argName}' argument should be a string, but got {GetTypes(jsArg)}"), JSValueType.ExecutionError);
            return false;
        }

        exception = default;
        val = jsArg.AsString;
        return true;
    }

    private bool GetIdAndDocFromArg(T self, string signature, out string Id, out BlittableJsonReaderObject Doc, out T exception)
    {
        if (self.IsObject && self.AsObject() is IBlittableObjectInstance<T> doc)
        {
            Id = doc.DocumentId;
            Doc = doc.Blittable;
            exception = default;
            return true;
        }

        if (self.IsStringEx)
        {
            var id = self.AsString;
            var document = _database.DocumentsStorage.Get(_docsCtx, id);
            if (document == null)
            {
                Id = null;
                Doc = null;
                exception = CreateErrorAndSetLastExceptionIfNeeded(new DocumentDoesNotExistException(id, "Cannot operate on a missing document."), JSValueType.ExecutionError);
                return false;
            }

            Id = id;
            Doc = document.Data;
            exception = default;
            return true;
        }

        Id = null;
        Doc = null;
        exception = CreateErrorAndSetLastExceptionIfNeeded(
            new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself. {GetTypes(self)}"),
            JSValueType.ExecutionError);
        return false;
    }

    public T ExplodeArgs(T self, T[] args)
    {
        if (args.Length != 2)
            return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("Raven_ExplodeArgs(this, args) - must be called with 2 arguments"), JSValueType.ExecutionError);

        if (args[1].IsObject && args[1].Object is IBlittableObjectInstance<T> boi)
        {
            SetArgs(args, boi);
            return self;
        }
        if (args[1].IsNull || args[1].IsUndefined)
            return self;// noop

        return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("Raven_ExplodeArgs(this, args) second argument must be BlittableObjectInstance"), JSValueType.ExecutionError);
    }

    protected abstract string GetTypes(T value);
    protected abstract void SetArgs(T[] args, IBlittableObjectInstance<T> boi);

    public T Raven_Min(T self, T[] args)
    {
        if (GenericSortTwoElementArray(args, out var exception) == false)
        {
            return exception;
        }

        return args[0].IsNull ? args[1] : args[0];
    }

    public T Raven_Max(T self, T[] args)
    {
        if (GenericSortTwoElementArray(args, out var exception) == false)
        {
            return exception;
        }

        return args[1];
    }

    private bool GenericSortTwoElementArray(IList<T> args, out T exception, [CallerMemberName] string caller = null)
    {
        void Swap()
        {
            var tmp = args[1];
            args[1] = args[0];
            args[0] = tmp;
        }

        // this is basically the same as Math.min / Math.max, but
        // can also be applied to strings, numbers and nulls

        if (args.Count != 2)
        {
            exception = CreateErrorAndSetLastExceptionIfNeeded(new ArgumentException(caller + "must be called with exactly two arguments"), JSValueType.ExecutionError);
            return false;
        }

        switch (args[0].ValueType)
        {
            case JSValueType.Uninitialized:
            case JSValueType.Undefined:
            case JSValueType.Null:
                // null sorts lowers, so that is fine (either the other one is null or
                // already higher than us).
                break;
            case JSValueType.Bool:
            case JSValueType.Number:
            case JSValueType.NumberObject:
            case JSValueType.Int32:
                var a = args[0].AsDouble;
                var b = args[1].AsDouble;
                if (a > b)
                    Swap();
                break;
            case JSValueType.String:
                switch (args[1].ValueType)
                {
                    case JSValueType.Uninitialized:
                    case JSValueType.Undefined:
                    case JSValueType.Null:
                        Swap(); // a value is bigger than no value
                        break;
                    case JSValueType.Bool:
                    case JSValueType.Number:
                    case JSValueType.NumberObject:
                    case JSValueType.Int32:
                        // if the string value is a number that is smaller than
                        // the numeric value, because Math.min(true, "-2") works :-(
                        if (double.TryParse(args[0].AsString, out double d) == false ||
                            d > args[1].AsDouble)
                        {
                            Swap();
                        }

                        break;
                    case JSValueType.String:
                        if (string.Compare(args[0].AsString, args[1].AsString, StringComparison.InvariantCultureIgnoreCase) > 0)
                            Swap();
                        break;
                }

                break;
            case JSValueType.Object:
                exception = CreateErrorAndSetLastExceptionIfNeeded(new ArgumentException(caller + " cannot be called on an object"), JSValueType.ExecutionError);
                return false;
            //TODO: egor maybe make default here ?
        }

        exception = default;
        return true;
    }

    public T ThrowOnLoadDocument(T self, T[] args)
    {
        return CreateErrorAndSetLastExceptionIfNeeded(new MissingMethodException("The method LoadDocument was renamed to 'load'"), JSValueType.ExecutionError);
    }

    public T ThrowOnDeleteDocument(T self, T[] args)
    {
        return CreateErrorAndSetLastExceptionIfNeeded(new MissingMethodException("The method DeleteDocument was renamed to 'del'"), JSValueType.ExecutionError);
    }

    public T ThrowOnPutDocument(T self, T[] args)
    {
        return CreateErrorAndSetLastExceptionIfNeeded(new MissingMethodException("The method PutDocument was renamed to 'put'"), JSValueType.ExecutionError);
    }

    private T ThrowInvalidIncrementCounterArgs(T[] args)
    {
        return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"There is no overload of method 'incrementCounter' that takes {args.Length} arguments." +
                                            "Supported overloads are : 'incrementCounter(doc, name)' , 'incrementCounter(doc, name, value)'"), JSValueType.ExecutionError);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T TranslateToJs(JsonOperationContext context, object o, bool keepAlive = false)
    {
        return JsUtils.TranslateToJs(context, o, keepAlive);
    }

    public T[] _args = Array.Empty<T>();

    public void SetArgs(JsonOperationContext jsonCtx, string method, object[] args)
    {
        if (_args.Length != args.Length)
            _args = new T[args.Length];

        for (var i = 0; i < args.Length; i++)
            _args[i] = TranslateToJs(jsonCtx, args[i], keepAlive: false);

        if (method != QueryMetadata.SelectOutput && _args.Length == 2 && _args[1].IsObject &&
            _args[1].AsObject() is IBlittableObjectInstance<T>)
        {
            SetArgsInternal();
        }
    }

    public abstract void SetArgsInternal();

    private static readonly TimeSeriesStorage.AppendOptions AppendOptionsForScript = new TimeSeriesStorage.AppendOptions { AddNewNameToMetadata = false };

    public IScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, object[] args, QueryTimingsScope scope = null,
        CancellationToken token = default)
    {
        return Run(jsonCtx, docCtx, method, null, args, scope, token);
    }

    public abstract IScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, string documentId, object[] args,
        QueryTimingsScope scope = null, CancellationToken token = default);

    public bool GetTimeSeriesFunctionArgs(string name, T[] args, out string docId, out List<IDisposable> lazyIds, out object[] val, out T exception)
    {
        var tsFunctionArgs = new object[args.Length + 1];
        docId = null;

        lazyIds = new List<IDisposable>();

        for (var index = 0; index < args.Length; index++)
        {
            if (args[index].Object is IBlittableObjectInstance<T> boi)
            {
                var lazyId = _docsCtx.GetLazyString(boi.DocumentId);
                lazyIds.Add(lazyId);
                tsFunctionArgs[index] = new Document {Data = boi.Blittable, Id = lazyId};

                if (index == 0)
                {
                    // take the Id of the document to operate on
                    // from the first argument (it can be a different document than the original doc)
                    docId = boi.DocumentId;
                }
            }
            else
            {
                tsFunctionArgs[index] = Translate(args[index], _jsonCtx);
            }
        }

        if (docId == null)
        {
            if (_args[0].IsObject == false ||
                !(_args[0].Object is IBlittableObjectInstance<T> originalDoc))
            {
                exception = CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException(
                    $"Failed to invoke time series function '{name}'. Couldn't find the document ID to operate on. " +
                    "A Document instance argument was not provided to the time series function or to the ScriptRunner"), JSValueType.ExecutionError);
                val = null;
                return false;
            }

            docId = originalDoc.DocumentId;
        }

        if (_args[_args.Length - 1].IsObject == false || !(_args[_args.Length - 1].Object is IBlittableObjectInstance<T> queryParams))
        {
            exception = CreateErrorAndSetLastExceptionIfNeeded(
                new InvalidOperationException($"Failed to invoke time series function '{name}'. ScriptRunner is missing QueryParameters argument"),
                JSValueType.ExecutionError);
            val = null;
            return false;
        }

        tsFunctionArgs[tsFunctionArgs.Length - 1] = new Document {Data = queryParams.Blittable};

        val = tsFunctionArgs;
        exception = default;
        return true;
    }


    public object Translate(JsonOperationContext context, object o)
    {
        return JsUtils.TranslateToJs(context, o);
    }

    public T CreateEmptyObject()
    {
        return EngineHandle.CreateObject();
    }

    public object Translate(IScriptRunnerResult result, JsonOperationContext context, IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
    {
        //TODO: egor handle duplicate code 
        return result.TranslateRawJsValue(context, modifier, usageMode);
    }

    internal object Translate(T jsValue, JsonOperationContext context, IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None, bool isRoot = true)
    {
        if (jsValue.IsStringEx)
            return jsValue.AsString;
        if (jsValue.IsBoolean)
            return jsValue.AsBoolean;
        if (jsValue.IsArray)
        {
            RuntimeHelpers.EnsureSufficientExecutionStack();
            var list = new List<object>();
            for (int i = 0; i < jsValue.ArrayLength; i++)
            {
                using (var jsItem = jsValue.GetProperty(i))
                {
                    list.Add(Translate(jsItem, context, modifier, usageMode, isRoot: false));
                }
            }
            return list;
        }
        if (jsValue.IsObject)
        {
            if (jsValue.IsNull)
                return null;
            return JsBlittableBridge.Translate(context, jsValue, modifier, usageMode, isRoot: isRoot);
        }
        if (jsValue.IsNumberOrIntEx)
            return jsValue.AsDouble;
        if (jsValue.IsNull || jsValue.IsUndefined)
            return null;
        return CreateErrorAndSetLastExceptionIfNeeded(new NotSupportedException("Unable to translate " + jsValue.ValueType), JSValueType.ExecutionError);
    }

    public override string ToString()
    {
        return string.Join(Environment.NewLine, _runner.ScriptsSource);
    }

    protected T AssertValidId()
    {
        return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("The first parameter to put(id, doc, changeVector) must be a string"), JSValueType.ExecutionError);
    }

    protected bool AssertNotReadOnly(out T val)
    {
        if (ReadOnly)
        {
            val = CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("Cannot make modifications in readonly context"), JSValueType.ExecutionError);
            return false;
        }

        val = default;
        return true;
    }

    protected bool AssertValidDatabaseContext(string functionName, out T val)
    {
        if (_docsCtx == null)
        {
            val = CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"Unable to use `{functionName}` when this instance is not attached to a database operation"),
                JSValueType.ExecutionError);
            return false;
        }

        val = default;
        return true;
    }

    protected T ThrowInvalidCounterValue()
    {
       return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("incrementCounter(doc, name, value): 'value' must be a number argument"), JSValueType.ExecutionError);
    }

    protected T ThrowInvalidCounterName(string signature)
    {
        return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"{signature}: 'name' must be a non-empty string argument"), JSValueType.ExecutionError);
    }

    protected T ThrowInvalidDocumentArgsType(string signature)
    {
        return  CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself"), JSValueType.ExecutionError);
    }

    protected T ThrowMissingDocument(string id)
    {
       return CreateErrorAndSetLastExceptionIfNeeded(new DocumentDoesNotExistException(id, "Cannot operate on counters of a missing document."), JSValueType.ExecutionError);
    }

    protected T ThrowDeleteCounterNameArg()
    {
        return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("deleteCounter(doc, name): 'name' must be a string argument"), JSValueType.ExecutionError);
    }

    protected T ThrowInvalidDeleteCounterDocumentArg()
    {
        return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("deleteCounter(doc, name): 'doc' must be a string argument (the document id) or the actual document instance itself"), JSValueType.ExecutionError);
    }

    protected T ThrowInvalidDeleteCounterArgs()
    {
        return CreateErrorAndSetLastExceptionIfNeeded(new InvalidOperationException("deleteCounter(doc, name) must be called with exactly 2 arguments"), JSValueType.ExecutionError);
    }

    protected static JsonOperationContext ThrowArgumentNull()
    {
        throw new ArgumentNullException("jsonCtx");
    }

    protected void Reset()
    {
        if (DebugMode)
        {
            if (DebugOutput == null)
                DebugOutput = new List<string>();
            if (DebugActions == null)
                DebugActions = new PatchDebugActions();
        }

        Includes?.Clear();
        IncludeRevisionsChangeVectors?.Clear();
        IncludeRevisionByDateTimeBefore = null;
        CompareExchangeValueIncludes?.Clear();
        DocumentCountersToUpdate?.Clear();
        DocumentTimeSeriesToUpdate?.Clear();
        PutOrDeleteCalled = false;
        OriginalDocumentId = null;
        RefreshOriginalDocument = false;

        EngineHandle.ResetCallStack();
        EngineHandle.ResetConstraints();
    }

    protected abstract void DisposeArgs();

    protected abstract JavaScriptException CreateFullError(Exception e);

}

public interface ISingleRun
{
    public void CleanStuff();

    public IScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, object[] args, QueryTimingsScope scope = null,
        CancellationToken token = default);

    public IScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, string documentId, object[] args,
        QueryTimingsScope scope = null, CancellationToken token = default);

    public bool ReadOnly { get; set; }

    public IScriptEngineChanges ScriptEngineHandle { get; set; }

    public object Translate(IScriptRunnerResult result, JsonOperationContext context, IResultModifier modifier = null,
        BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None);

    public object Translate(JsonOperationContext context, object o);
    public bool DebugMode { get; set; }
    public HashSet<string> DocumentCountersToUpdate { get; set; }
    public HashSet<string> DocumentTimeSeriesToUpdate { get; set; }
    public HashSet<string> Includes { get; set; }
    public HashSet<string> IncludeRevisionsChangeVectors { get; set; }
    public DateTime? IncludeRevisionByDateTimeBefore { get; set; }
    public HashSet<string> CompareExchangeValueIncludes { get; set; }
    public List<string> DebugOutput { get; set; }
}
