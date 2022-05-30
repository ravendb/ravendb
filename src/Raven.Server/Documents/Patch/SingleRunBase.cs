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
    public Exception LastException { get; set; }
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
    }

    public IJsEngineHandle<T> ScriptEngineHandle;
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
    IScriptEngineChanges ISingleRun.ScriptEngineHandle { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

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

        //  Initialize(executeScriptsSource);
    }

    public void Initialize(bool executeScriptsSource = true)
    {
        lock (ScriptEngineHandle)
        {
            ScriptEngineHandle.SetGlobalClrCallBack("getMetadata", JsUtils.GetMetadata);
            ScriptEngineHandle.SetGlobalClrCallBack("metadataFor", (JsUtils.GetMetadata));
            ScriptEngineHandle.SetGlobalClrCallBack("id", JsUtils.GetDocumentId);

            //console.log
            var consoleObject = ScriptEngineHandle.CreateObject();
            var jsFuncLog = ScriptEngineHandle.CreateClrCallBack("log", OutputDebug, keepAlive: true);
            consoleObject.FastAddProperty("log", jsFuncLog, false, false, false);
            ScriptEngineHandle.SetGlobalProperty("console", consoleObject);

            //spatial.distance
            var spatialObject = ScriptEngineHandle.CreateObject();
            var jsFuncSpatial = ScriptEngineHandle.CreateClrCallBack("distance", Spatial_Distance, keepAlive: true);
            spatialObject.FastAddProperty("distance", jsFuncSpatial.Clone(), false, false, false);
            ScriptEngineHandle.SetGlobalProperty("spatial", spatialObject);
            ScriptEngineHandle.SetGlobalProperty("spatial.distance", jsFuncSpatial);

            // includes
            var includesObject = ScriptEngineHandle.CreateObject();
            var jsFuncIncludeDocument = ScriptEngineHandle.CreateClrCallBack("include", IncludeDoc, keepAlive: true);
            includesObject.FastAddProperty("document", jsFuncIncludeDocument.Clone(), false, false, false);
            // includes - backward compatibility
            ScriptEngineHandle.SetGlobalProperty("include", jsFuncIncludeDocument);

            var jsFuncIncludeCompareExchangeValue =
                ScriptEngineHandle.CreateClrCallBack("cmpxchg", IncludeCompareExchangeValue, keepAlive: true);
            includesObject.FastAddProperty("cmpxchg", jsFuncIncludeCompareExchangeValue, false, false, false);

            var jsFuncIncludeRevisions = ScriptEngineHandle.CreateClrCallBack("revisions", IncludeRevisions, keepAlive: true);
            includesObject.FastAddProperty("revisions", jsFuncIncludeRevisions, false, false, false);
            ScriptEngineHandle.SetGlobalProperty("includes", includesObject);

            ScriptEngineHandle.SetGlobalClrCallBack("output", OutputDebug);
            ScriptEngineHandle.SetGlobalClrCallBack("load", LoadDocument);
            ScriptEngineHandle.SetGlobalClrCallBack("LoadDocument", ThrowOnLoadDocument);

            ScriptEngineHandle.SetGlobalClrCallBack("loadPath", LoadDocumentByPath);
            ScriptEngineHandle.SetGlobalClrCallBack("del", DeleteDocument);
            ScriptEngineHandle.SetGlobalClrCallBack("DeleteDocument", ThrowOnDeleteDocument);
            ScriptEngineHandle.SetGlobalClrCallBack("put", PutDocument);
            ScriptEngineHandle.SetGlobalClrCallBack("PutDocument", ThrowOnPutDocument);
            ScriptEngineHandle.SetGlobalClrCallBack("cmpxchg", CompareExchange);

            ScriptEngineHandle.SetGlobalClrCallBack("counter", GetCounter);
            ScriptEngineHandle.SetGlobalClrCallBack("counterRaw", GetCounterRaw);
            ScriptEngineHandle.SetGlobalClrCallBack("incrementCounter", IncrementCounter);
            ScriptEngineHandle.SetGlobalClrCallBack("deleteCounter", DeleteCounter);

            ScriptEngineHandle.SetGlobalClrCallBack("lastModified", GetLastModified);

            ScriptEngineHandle.SetGlobalClrCallBack("startsWith", StartsWith);
            ScriptEngineHandle.SetGlobalClrCallBack("endsWith", EndsWith);
            ScriptEngineHandle.SetGlobalClrCallBack("regex", Regex);

            ScriptEngineHandle.SetGlobalClrCallBack("Raven_ExplodeArgs", ExplodeArgs);
            ScriptEngineHandle.SetGlobalClrCallBack("Raven_Min", Raven_Min);
            ScriptEngineHandle.SetGlobalClrCallBack("Raven_Max", Raven_Max);

            ScriptEngineHandle.SetGlobalClrCallBack("convertJsTimeToTimeSpanString", ConvertJsTimeToTimeSpanString);
            ScriptEngineHandle.SetGlobalClrCallBack("convertToTimeSpanString", ConvertToTimeSpanString);
            ScriptEngineHandle.SetGlobalClrCallBack("compareDates", CompareDates);

            ScriptEngineHandle.SetGlobalClrCallBack("toStringWithFormat", ToStringWithFormat);

            ScriptEngineHandle.SetGlobalClrCallBack("scalarToRawString", ScalarToRawString);

            //TimeSeries
            ScriptEngineHandle.SetGlobalClrCallBack("timeseries", TimeSeries);
            ScriptEngineHandle.Execute(ScriptRunnerCache.PolyfillJs, "polyfill.js");

            if (executeScriptsSource)
            {
                ExecuteScriptsSource();
            }

            foreach (var ts in _runner.TimeSeriesDeclaration)
            {
                ScriptEngineHandle.SetGlobalClrCallBack(ts.Key,
                    (
                        (self, args) => InvokeTimeSeriesFunction(ts.Key, args)
                    )
                );
            }
        }
    }

    public void ExecuteScriptsSource()
    {
        foreach (var script in _scriptsSource)
        {
            try
            {
                ScriptEngineHandle.Execute(script);
            }
            catch (Exception e)
            {
                throw new JavaScriptParseException("Failed to parse: " + Environment.NewLine + script, e);
            }
        }
    }

    public T LoadDocument(T self, T[] args)
    {
        //  throw new NotImplementedException();
        using (_loadScope = _loadScope?.Start() ?? _scope?.For(nameof(QueryTimingsScope.Names.Load)))
        {
            AssertValidDatabaseContext("load");

            if (args.Length != 1)
                throw new InvalidOperationException($"load(id | ids) must be called with a single string argument");

            if (args[0].IsNull || args[0].IsUndefined)
                return args[0];

            if (args[0].IsArray)
            {
                var results = new List<T>();
                var jsArray = args[0];
                int arrayLength = jsArray.ArrayLength;
                for (int i = 0; i < arrayLength; ++i)
                {
                    using (var jsItem = jsArray.GetProperty(i))
                    {
                        if (jsItem.IsStringEx == false)
                            throw new InvalidOperationException("load(ids) must be called with a array of strings, but got " + jsItem.ValueType + " - " + jsItem);

                        results.Add(LoadDocumentInternal(jsItem.AsString));
                    }
                }
                
                return ScriptEngineHandle.CreateArray(results);
            }

            if (args[0].IsStringEx == false)
                throw new InvalidOperationException("load(id | ids) must be called with a single string or array argument");

            return LoadDocumentInternal(args[0].AsString);
        }
    }

    public T LoadDocumentByPath(T self, T[] args)
    {
        using (_loadScope = _loadScope?.Start() ?? _scope?.For(nameof(QueryTimingsScope.Names.Load)))
        {
            AssertValidDatabaseContext("loadPath");

            if (args.Length != 2 ||
                (args[0].IsNull == false && args[0].IsUndefined == false && args[0].IsObject == false)
                || args[1].IsString == false)
                throw new InvalidOperationException("loadPath(doc, path) must be called with a document and path");

            if (args[0].IsNull || args[1].IsUndefined)
                return args[0];

            if (args[0].AsObject() is IBlittableObjectInstance b)
            {
                var path = args[1].AsString;
                if (_documentIds == null)
                    _documentIds = new HashSet<string>();

                _documentIds.Clear();
                IncludeUtil.GetDocIdFromInclude(b.Blittable, path, _documentIds, _database.IdentityPartsSeparator);
                if (path.IndexOf("[]", StringComparison.InvariantCulture) != -1)
                {
                    return ScriptEngineHandle.FromObjectGen(_documentIds.Select(LoadDocumentInternal).ToList());
                } // array

                if (_documentIds.Count == 0)
                    return ScriptEngineHandle.Null;

                return LoadDocumentInternal(_documentIds.First());
            }

            throw new InvalidOperationException("loadPath(doc, path) must be called with a valid document instance, but got a JS object instead");
        }
    }

    private T LoadDocumentInternal(string id)
    {
        if (string.IsNullOrEmpty(id))
        {
            return ScriptEngineHandle.Undefined;
        }

        var document = _database.DocumentsStorage.Get(_docsCtx, id);
        if (DebugMode)
        {
            DebugActions.LoadDocument.Add(new DynamicJsonValue { ["Id"] = id, ["Exists"] = document != null });
        }

        return JsUtils.TranslateToJs(_jsonCtx, document, keepAlive: true);
    }


    public T DeleteDocument(T self, T[] args)
    {
        if (args.Length != 1 && args.Length != 2)
            throw new InvalidOperationException("delete(id, changeVector) must be called with at least one parameter");

        if (args[0].IsString == false)
            throw new InvalidOperationException("delete(id, changeVector) id argument must be a string");

        var id = args[0].AsString;
        string changeVector = null;

        if (args.Length == 2 && args[1].IsString)
            changeVector = args[1].AsString;

        PutOrDeleteCalled = true;
        AssertValidDatabaseContext("delete document");
        AssertNotReadOnly();

        var result = _database.DocumentsStorage.Delete(_docsCtx, id, changeVector);

        if (RefreshOriginalDocument && string.Equals(OriginalDocumentId, id, StringComparison.OrdinalIgnoreCase))
            RefreshOriginalDocument = false;

        if (DebugMode)
        {
            DebugActions.DeleteDocument.Add(id);
        }

        return ScriptEngineHandle.CreateValue(result != null);
    }

    public T PutDocument(T self, T[] args)
    {
        string changeVector = null;

        if (args.Length != 2 && args.Length != 3)
            throw new InvalidOperationException("put(id, doc, changeVector) must be called with called with 2 or 3 arguments only");
        AssertValidDatabaseContext("put document");
        AssertNotReadOnly();
        if (args[0].IsString == false && args[0].IsNull == false && args[0].IsUndefined == false)
            AssertValidId();

        var id = args[0].IsNull || args[0].IsUndefined ? null : args[0].AsString;

        if (args[1].IsObject == false)
            throw new InvalidOperationException(
                $"Created document must be a valid object which is not null or empty. Document ID: '{id}'.");

        PutOrDeleteCalled = true;

        if (args.Length == 3)
            if (args[2].IsString)
                changeVector = args[2].AsString;
            else if (args[2].IsNull == false && args[0].IsUndefined == false)
                throw new InvalidOperationException(
                    $"The change vector must be a string or null. Document ID: '{id}'.");

        BlittableJsonReaderObject reader = null;

        try
        {
            reader = JsBlittableBridge.Translate(_jsonCtx, args[1], usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);

            var put = _database.DocumentsStorage.Put(
                _docsCtx,
                id,
                _docsCtx.GetLazyString(changeVector),
                reader,
                //RavenDB-11391 Those flags were added to cause attachment/counter metadata table check & remove metadata properties if not necessary
                nonPersistentFlags: NonPersistentDocumentFlags.ResolveAttachmentsConflict | NonPersistentDocumentFlags.ResolveCountersConflict | NonPersistentDocumentFlags.ResolveTimeSeriesConflict
            );

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

            return ScriptEngineHandle.CreateValue(put.Id);
        }
        finally
        {
            if (DebugMode == false)
                reader?.Dispose();
        }
    }

    public T CompareExchange(T self, T[] args)
    {
        AssertValidDatabaseContext("cmpxchg");

        if (args.Length != 1 && args.Length != 2 || args[0].IsStringEx == false)
            throw new InvalidOperationException("cmpxchg(key) must be called with a single string argument");

        return CmpXchangeInternal(CompareExchangeKey.GetStorageKey(_database.Name, args[0].AsString));
    }

    private T CmpXchangeInternal(string key)
    {
        if (string.IsNullOrEmpty(key))
            return ScriptEngineHandle.Undefined;

        using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
        using (ctx.OpenReadTransaction())
        {
            var value = _database.ServerStore.Cluster.GetCompareExchangeValue(ctx, key).Value;
            if (value == null)
                return ScriptEngineHandle.Null;

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
        AssertValidDatabaseContext(signature);

        if (args.Length != 2)
            throw new InvalidOperationException($"{signature} must be called with exactly 2 arguments");

        string id;
        if (args[0].IsObject && args[0].AsObject() is IBlittableObjectInstance doc)
        {
            id = doc.DocumentId;
        }
        else if (args[0].IsStringEx)
        {
            id = args[0].AsString;
        }
        else
        {
            throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself");
        }

        if (args[1].IsStringEx == false)
        {
            throw new InvalidOperationException($"{signature}: 'name' must be a string argument");
        }

        var name = args[1].AsString;
        if (string.IsNullOrEmpty(id) || string.IsNullOrEmpty(name))
        {
            return ScriptEngineHandle.Undefined;
        }

        if (raw == false)
        {
            T counterValue;
            bool exists = false;
            var counterValue1 = _database.DocumentsStorage.CountersStorage.GetCounterValue(_docsCtx, id, name);
            if (counterValue1.HasValue)
            {
                var val123 = counterValue1.Value;
                var actualValue = val123.Value;

                counterValue = ScriptEngineHandle.CreateValue(actualValue);
                exists = true;
            }
            else
            {
                counterValue = ScriptEngineHandle.Null;
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

        var obj = ScriptEngineHandle.CreateObject();
        foreach (var partialValue in _database.DocumentsStorage.CountersStorage.GetCounterPartialValues(_docsCtx, id, name))
        {
            obj.FastAddProperty(partialValue.ChangeVector, ScriptEngineHandle.CreateValue(partialValue.PartialValue), writable: true, enumerable: false, configurable: false);
        }

        return obj;
    }

    public T IncrementCounter(T self, T[] args)
    {
        AssertValidDatabaseContext("incrementCounter");

        if (args.Length < 2 || args.Length > 3)
        {
            ThrowInvalidIncrementCounterArgs(args);
        }

        var signature = args.Length == 2 ? "incrementCounter(doc, name)" : "incrementCounter(doc, name, value)";

        BlittableJsonReaderObject docBlittable = null;
        string id = null;

        if (args[0].IsObject && args[0].AsObject() is IBlittableObjectInstance doc)
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
                ThrowMissingDocument(id);
                Debug.Assert(false); // never hit
            }

            docBlittable = document.Data;
        }
        else
        {
            ThrowInvalidDocumentArgsType(signature);
        }

        Debug.Assert(id != null && docBlittable != null);

        if (args[1].IsStringEx == false)
            ThrowInvalidCounterName(signature);

        var name = args[1].AsString;
        if (string.IsNullOrWhiteSpace(name))
            ThrowInvalidCounterName(signature);

        double value = 1;
        if (args.Length == 3)
        {
            if (args[2].IsNumber == false)
                ThrowInvalidCounterValue();
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

        return ScriptEngineHandle.True;
    }

    public T DeleteCounter(T self, T[] args)
    {
        AssertValidDatabaseContext("deleteCounter");

        if (args.Length != 2)
        {
            ThrowInvalidDeleteCounterArgs();
        }

        string id = null;
        BlittableJsonReaderObject docBlittable = null;

        //args[0].TryGetDocumentIdAndBlittable
        if (args[0].IsObject && args[0].AsObject() is IBlittableObjectInstance doc)
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
                ThrowMissingDocument(id);
                Debug.Assert(false); // never hit
            }

            docBlittable = document.Data;
        }
        else
        {
            ThrowInvalidDeleteCounterDocumentArg();
        }

        Debug.Assert(id != null && docBlittable != null);

        if (args[1].IsStringEx == false)
        {
            ThrowDeleteCounterNameArg();
        }

        var name = args[1].AsString;
        _database.DocumentsStorage.CountersStorage.DeleteCounter(_docsCtx, id, CollectionName.GetCollectionName(docBlittable), name);

        DocumentCountersToUpdate ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        DocumentCountersToUpdate.Add(id);

        if (DebugMode)
        {
            DebugActions.DeleteCounter.Add(name);
        }

        return ScriptEngineHandle.True;
    }

    public T GetLastModified(T self, T[] args)
    {
        if (args.Length != 1)
            throw new InvalidOperationException("lastModified(doc) must be called with a single argument");

        if (args[0].IsNull || args[0].IsUndefined)
            return args[0];

        if (args[0].IsObject == false)
            throw new InvalidOperationException("lastModified(doc) must be called with an object argument");

        if (args[0].AsObject() is IBlittableObjectInstance doc)
        {
            if (doc.LastModified == null)
                return ScriptEngineHandle.Undefined;

            // we use UTC because last modified is in UTC
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            var jsTime = doc.LastModified.Value.Subtract(epoch).TotalMilliseconds;
            return ScriptEngineHandle.CreateValue(jsTime);
        }

        return ScriptEngineHandle.Undefined;
    }

    public T StartsWith(T self, T[] args)
    {
        if (args.Length != 2 || args[0].IsStringEx == false || args[1].IsStringEx == false)
            throw new InvalidOperationException("startsWith(text, contained) must be called with two string parameters");

        return ScriptEngineHandle.CreateValue(args[0].AsString.StartsWith(args[1].AsString, StringComparison.OrdinalIgnoreCase));
    }

    public T EndsWith(T self, T[] args)
    {
        if (args.Length != 2 || args[0].IsStringEx == false || args[1].IsStringEx == false)
            throw new InvalidOperationException("endsWith(text, contained) must be called with two string parameters");

        return ScriptEngineHandle.CreateValue(args[0].AsString.EndsWith(args[1].AsString, StringComparison.OrdinalIgnoreCase));
    }

    public T Regex(T self, T[] args)
    {
        if (args.Length != 2 || args[0].IsStringEx == false || args[1].IsStringEx == false)
            throw new InvalidOperationException("regex(text, regex) must be called with two string parameters");

        var regex = _regexCache.Get(args[1].AsString);

        return regex.IsMatch(args[0].AsString) ? ScriptEngineHandle.True : ScriptEngineHandle.False;
    }

    public T ConvertJsTimeToTimeSpanString(T self, T[] args)
    {
        if (args.Length != 1 || args[0].IsNumber == false)
            throw new InvalidOperationException("convertJsTimeToTimeSpanString(ticks) must be called with a single long argument");

        var ticks = Convert.ToInt64(args[0].AsDouble) * 10000;

        var asTimeSpan = new TimeSpan(ticks);

        return ScriptEngineHandle.CreateValue(asTimeSpan.ToString());
    }

    public T ConvertToTimeSpanString(T self, T[] args)
    {
        if (args.Length == 1)
        {
            if (args[0].IsNumber == false)
                throw new InvalidOperationException("convertToTimeSpanString(ticks) must be called with a single long argument");
            //TODO: egor how to make long? string then parse?
            var ticks = Convert.ToInt64(args[0].AsInt32);
            var asTimeSpan = new TimeSpan(ticks);
            return ScriptEngineHandle.CreateValue(asTimeSpan.ToString());
        }

        if (args.Length == 3)
        {
            if (args[0].IsNumber == false || args[1].IsNumber == false || args[2].IsNumber == false)
                throw new InvalidOperationException("convertToTimeSpanString(hours, minutes, seconds) must be called with integer values");

            var hours = Convert.ToInt32(args[0].AsInt32);
            var minutes = Convert.ToInt32(args[1].AsInt32);
            var seconds = Convert.ToInt32(args[2].AsInt32);

            var asTimeSpan = new TimeSpan(hours, minutes, seconds);
            return ScriptEngineHandle.CreateValue(asTimeSpan.ToString());
        }

        if (args.Length == 4)
        {
            if (args[0].IsNumber == false || args[1].IsNumber == false || args[2].IsNumber == false || args[3].IsNumber == false)
                throw new InvalidOperationException("convertToTimeSpanString(days, hours, minutes, seconds) must be called with integer values");

            var days = Convert.ToInt32(args[0].AsInt32);
            var hours = Convert.ToInt32(args[1].AsInt32);
            var minutes = Convert.ToInt32(args[2].AsInt32);
            var seconds = Convert.ToInt32(args[3].AsInt32);

            var asTimeSpan = new TimeSpan(days, hours, minutes, seconds);
            return ScriptEngineHandle.CreateValue(asTimeSpan.ToString());
        }

        if (args.Length == 5)
        {
            if (args[0].IsNumber == false || args[1].IsNumber == false || args[2].IsNumber == false || args[3].IsNumber == false || args[4].IsNumber == false)
                throw new InvalidOperationException("convertToTimeSpanString(days, hours, minutes, seconds, milliseconds) must be called with integer values");

            var days = Convert.ToInt32(args[0].AsInt32);
            var hours = Convert.ToInt32(args[1].AsInt32);
            var minutes = Convert.ToInt32(args[2].AsInt32);
            var seconds = Convert.ToInt32(args[3].AsInt32);
            var milliseconds = Convert.ToInt32(args[4].AsInt32);

            var asTimeSpan = new TimeSpan(days, hours, minutes, seconds, milliseconds);
            return ScriptEngineHandle.CreateValue(asTimeSpan.ToString());
        }

        throw new InvalidOperationException("supported overloads are: " +
                                            "convertToTimeSpanString(ticks), " +
                                            "convertToTimeSpanString(hours, minutes, seconds), " +
                                            "convertToTimeSpanString(days, hours, minutes, seconds), " +
                                            "convertToTimeSpanString(days, hours, minutes, seconds, milliseconds)");
    }

    public T CompareDates(T self, T[] args)
    {
        if (args.Length < 1 || args.Length > 3)
        {
            throw new InvalidOperationException($"No overload for method 'compareDates' takes {args.Length} arguments. " +
                                                "Supported overloads are : compareDates(date1, date2), compareDates(date1, date2, operationType)");
        }

        ExpressionType binaryOperationType;
        if (args.Length == 2)
        {
            binaryOperationType = ExpressionType.Subtract;
        }
        else if (args[2].IsStringEx == false ||
                 Enum.TryParse(args[2].AsString, out binaryOperationType) == false)
        {
            throw new InvalidOperationException("compareDates(date1, date2, operationType) : 'operationType' must be a string argument representing a valid 'ExpressionType'");
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
            date1 = GetDateArg(args[0], signature, "date1");
            date2 = GetDateArg(args[1], signature, "date2");
        }

        switch (binaryOperationType)
        {
            case ExpressionType.Subtract:
                return ScriptEngineHandle.CreateValue((date1 - date2).ToString());
            case ExpressionType.GreaterThan:
                return date1 > date2 ? ScriptEngineHandle.True : ScriptEngineHandle.False;
            case ExpressionType.GreaterThanOrEqual:
                return date1 >= date2 ? ScriptEngineHandle.True : ScriptEngineHandle.False;
            case ExpressionType.LessThan:
                return date1 < date2 ? ScriptEngineHandle.True : ScriptEngineHandle.False;
            case ExpressionType.LessThanOrEqual:
                return date1 <= date2 ? ScriptEngineHandle.True : ScriptEngineHandle.False;
            case ExpressionType.Equal:
                return date1 == date2 ? ScriptEngineHandle.True : ScriptEngineHandle.False;
            case ExpressionType.NotEqual:
                return date1 != date2 ? ScriptEngineHandle.True : ScriptEngineHandle.False;
            default:
                throw new InvalidOperationException($"compareDates(date1, date2, binaryOp) : unsupported binary operation '{binaryOperationType}'");
        }
    }

    public unsafe DateTime GetDateArg(T arg, string signature, string argName)
    {
        if (arg.IsDate)
        {
            return arg.AsDate;
        }

        if (arg.IsStringEx == false)
            ThrowInvalidDateArgument();

        var s = arg.AsString;
        fixed (char* pValue = s)
        {
            var result = LazyStringParser.TryParseDateTime(pValue, s.Length, out DateTime dt, out _, properlyParseThreeDigitsMilliseconds: true);
            if (result != LazyStringParser.Result.DateTime)
                ThrowInvalidDateArgument();

            return dt;
        }

        void ThrowInvalidDateArgument() =>
            throw new ArgumentException($"{signature} : {argName} must be of type 'DateInstance' or a DateTime string. {GetTypes(arg)}");
    }

    public unsafe T ToStringWithFormat(T self, T[] args)
    {
        if (args.Length < 1 || args.Length > 3)
        {
            throw new InvalidOperationException($"No overload for method 'toStringWithFormat' takes {args.Length} arguments. " +
                                                "Supported overloads are : toStringWithFormat(object), toStringWithFormat(object, format), toStringWithFormat(object, culture), toStringWithFormat(object, format, culture).");
        }

        var cultureInfo = CultureInfo.InvariantCulture;
        string format = null;

        for (var i = 1; i < args.Length; i++)
        {
            if (args[i].IsStringEx == false)
            {
                throw new InvalidOperationException("toStringWithFormat : 'format' and 'culture' must be string arguments");
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
                ScriptEngineHandle.CreateValue(date.ToString(format, cultureInfo)) :
                ScriptEngineHandle.CreateValue(date.ToString(cultureInfo));
        }

        if (args[0].IsNumberOrIntEx)
        {
            var num = args[0].AsDouble;
            return format != null ?
                ScriptEngineHandle.CreateValue(num.ToString(format, cultureInfo)) :
                ScriptEngineHandle.CreateValue(num.ToString(cultureInfo));
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
                            ScriptEngineHandle.CreateValue(dt.ToString(format, cultureInfo)) :
                            ScriptEngineHandle.CreateValue(dt.ToString(cultureInfo));
                    default:
                        throw new InvalidOperationException("toStringWithFormat(dateString) : 'dateString' is not a valid DateTime string");
                }
            }
        }

        if (args[0].IsBoolean == false)
        {
            throw new InvalidOperationException($"toStringWithFormat() is not supported for objects of type {args[0].ValueType} ");
        }

        var boolean = args[0].AsBoolean;
        return ScriptEngineHandle.CreateValue(boolean.ToString(cultureInfo));
    }

    public T ScalarToRawString(T self, T[] args)
    {
        if (args.Length != 2)
            throw new InvalidOperationException("scalarToRawString(document, lambdaToField) may be called on with two parameters only");

        T firstParam = args[0];
        if (firstParam.IsObject && args[0].AsObject() is IBlittableObjectInstance selfInstance)
        {
            T secondParam = args[1];
            if (secondParam.IsObject && secondParam.AsObject() is ScriptFunctionInstance lambda)
            {
                var functionAst = lambda.FunctionDeclaration;
                var propName = functionAst.TryGetFieldFromSimpleLambdaExpression();

                if (TryGetValueFromBoi(selfInstance, propName, out IBlittableObjectProperty<T> existingValue, out _) && existingValue != null)
                {
                    if (existingValue.Changed)
                    {
                        return existingValue.ValueHandle;
                    }
                }

                var propertyIndex = selfInstance.Blittable.GetPropertyIndex(propName);

                if (propertyIndex == -1)
                {
                    return ScriptEngineHandle.CreateObject();
                }

                BlittableJsonReaderObject.PropertyDetails propDetails = new BlittableJsonReaderObject.PropertyDetails();
                selfInstance.Blittable.GetPropertyByIndex(propertyIndex, ref propDetails);
                var value = propDetails.Value;

                switch (propDetails.Token & BlittableJsonReaderBase.TypesMask)
                {
                    case BlittableJsonToken.Null:
                        return ScriptEngineHandle.Null;
                    case BlittableJsonToken.Boolean:
                        return (bool)value ? ScriptEngineHandle.True : ScriptEngineHandle.False;
                    case BlittableJsonToken.Integer:
                        return ScriptEngineHandle.CreateValue((int)value);
                    case BlittableJsonToken.LazyNumber:
                    case BlittableJsonToken.String:
                    case BlittableJsonToken.CompressedString:
                        return CreateObjectBinder(value);
                    default:
                        throw new InvalidOperationException("scalarToRawString(document, lambdaToField) lambda to field must return either raw numeric or raw string types");
                }
            }

            throw new InvalidOperationException("scalarToRawString(document, lambdaToField) must be called with a second lambda argument");
        }

        throw new InvalidOperationException("scalarToRawString(document, lambdaToField) may be called with a document first parameter only");
    }

    public abstract void SetContext();
    protected abstract T CreateObjectBinder(object value);

    protected abstract bool TryGetValueFromBoi(IBlittableObjectInstance iboi, string propName, out IBlittableObjectProperty<T> blittableObjectProperty, out bool b);

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
            if (obj.AsObject() is IBlittableObjectInstance boi && boi.Changed == false)
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
            throw new ArgumentException("Called with expected number of arguments, expected: spatial.distance(lat1, lng1, lat2, lng2, kilometers | miles | cartesian)");

        for (int i = 0; i < 4; i++)
        {
            if (args[i].IsNumber == false)
                return ScriptEngineHandle.Undefined;
        }

        var lat1 = args[0].AsDouble;
        var lng1 = args[1].AsDouble;
        var lat2 = args[2].AsDouble;
        var lng2 = args[3].AsDouble;

        var units = SpatialUnits.Kilometers;
        if (args.Length > 4 && args[4].IsStringEx)
        {
            if (string.Equals("cartesian", args[4].AsString, StringComparison.OrdinalIgnoreCase))
                return ScriptEngineHandle.CreateValue(SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator.CartesianDistance(lat1, lng1, lat2, lng2));

            if (Enum.TryParse(args[4].AsString, ignoreCase: true, out units) == false)
                throw new ArgumentException("Unable to parse units " + args[5] + ", expected: 'kilometers' or 'miles'");
        }

        var result = SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator.HaverstineDistanceInMiles(lat1, lng1, lat2, lng2);
        if (units == SpatialUnits.Kilometers)
            result *= DistanceUtils.MILES_TO_KM;

        return ScriptEngineHandle.CreateValue(result);
    }

    public T IncludeDoc(T self, T[] args)
    {
        if (args.Length != 1)
            throw new InvalidOperationException("include(id) must be called with a single argument");

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
            throw new InvalidOperationException("include(doc) must be called with an string or string array argument");

        var id = args[0].AsString;

        if (Includes == null)
            Includes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        Includes.Add(id);

        return self;
    }

    public T IncludeCompareExchangeValue(T self, T[] args)
    {
        if (args.Length != 1)
            throw new InvalidOperationException("includes.cmpxchg(key) must be called with a single argument");

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
            throw new InvalidOperationException("includes.cmpxchg(key) must be called with an string or string array argument");

        var key = args[0].AsString;

        if (CompareExchangeValueIncludes == null)
            CompareExchangeValueIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        CompareExchangeValueIncludes.Add(key);

        return self;
    }

    public T IncludeRevisions(T self, T[] args)
    {
        if (args == null)
            return ScriptEngineHandle.Null;

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

        return ScriptEngineHandle.Null;
    }

    public T InvokeTimeSeriesFunction(string name, T[] args)
    {
        AssertValidDatabaseContext("InvokeTimeSeriesFunction");

        if (_runner.TimeSeriesDeclaration.TryGetValue(name, out var func) == false)
            throw new InvalidOperationException($"Failed to invoke time series function. Unknown time series name '{name}'.");

        var tsFunctionArgs = GetTimeSeriesFunctionArgs(name, args, out string docId, out var lazyIds);

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
        AssertValidDatabaseContext(_timeSeriesSignature);

        if (args.Length != 2)
            throw new ArgumentException($"{_timeSeriesSignature}: This method requires 2 arguments but was called with {args.Length}");

        var obj = ScriptEngineHandle.CreateObject();
        obj.SetProperty("append", ScriptEngineHandle.CreateClrCallBack("append", AppendTimeSeries));
        obj.SetProperty("increment", ScriptEngineHandle.CreateClrCallBack("increment", IncrementTimeSeries));
        obj.SetProperty("delete", ScriptEngineHandle.CreateClrCallBack("delete", DeleteRangeTimeSeries));
        obj.SetProperty("get", ScriptEngineHandle.CreateClrCallBack("get", GetRangeTimeSeries));
        obj.SetProperty("getStats", ScriptEngineHandle.CreateClrCallBack("getStats", GetStatsTimeSeries));
        obj.SetProperty("doc", args[0]);
        obj.SetProperty("name", args[1]);
        return obj;
    }

    private T GetRangeTimeSeries(T self, T[] args)
    {
        using var document = self.GetProperty("doc");
        using var name = self.GetProperty("name");
        AssertValidDatabaseContext("get");

        const string getRangeSignature = "get(from, to)";
        const string getAllSignature = "get()";

        var id = GetIdFromArg(document, _timeSeriesSignature);
        var timeSeries = GetStringArg(name, _timeSeriesSignature, "name");

        DateTime from, to;
        switch (args.Length)
        {
            case 0:
                from = DateTime.MinValue;
                to = DateTime.MaxValue;
                break;
            case 2:
                from = GetTimeSeriesDateArg(args[0], getRangeSignature, "from");
                to = GetTimeSeriesDateArg(args[1], getRangeSignature, "to");
                break;
            default:
                throw new ArgumentException($"'get' method has only the overloads: '{getRangeSignature}' or '{getAllSignature}', but was called with {args.Length} arguments.");
        }

        var reader = _database.DocumentsStorage.TimeSeriesStorage.GetReader(_docsCtx, id, timeSeries, from, to);

        var entries = new List<T>();
        foreach (var singleResult in reader.AllValues())
        {
            Span<double> valuesSpan = singleResult.Values.Span;
            var jsSpanItems = new T[valuesSpan.Length];
            for (int i = 0; i < valuesSpan.Length; i++)
            {
                jsSpanItems[i] = ScriptEngineHandle.CreateValue(valuesSpan[i]);
            }

            var entry = ScriptEngineHandle.CreateObject();
            entry.SetProperty(nameof(TimeSeriesEntry.Timestamp), ScriptEngineHandle.CreateValue(singleResult.Timestamp.GetDefaultRavenFormat(isUtc: true)));
            entry.SetProperty(nameof(TimeSeriesEntry.Tag), singleResult.Tag == null ? ScriptEngineHandle.Null : ScriptEngineHandle.CreateValue(singleResult.Tag.ToString()));
            entry.SetProperty(nameof(TimeSeriesEntry.Values), ScriptEngineHandle.CreateArray(jsSpanItems));
            entry.SetProperty(nameof(TimeSeriesEntry.IsRollup), ScriptEngineHandle.CreateValue(singleResult.Type == SingleResultType.RolledUp));

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

        if (DebugMode)
        {
            DebugActions.GetTimeSeries.Add(new DynamicJsonValue
            {
                ["Name"] = timeSeries,
                ["Exists"] = entries.Count != 0
            });
        }

        if (entries.Count == 0)
            return ScriptEngineHandle.CreateEmptyArray();

        return ScriptEngineHandle.CreateArray(entries);
    }

    private string GetIdFromArg(T docArg, string signature)
    {
        if (docArg.IsObject && docArg.AsObject() is IBlittableObjectInstance doc)
            return doc.DocumentId;

        if (docArg.IsStringEx == false)
            throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself. {GetTypes(docArg)}");

        var id = docArg.AsString;
        return id;

    }

    private T DeleteRangeTimeSeries(T self, T[] args)
    {
        using var document = self.GetProperty("doc");
        using var name = self.GetProperty("name");
        AssertValidDatabaseContext("timeseries(doc, name).delete");

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
                from = GetTimeSeriesDateArg(args[0], deleteSignature, "from");
                to = GetTimeSeriesDateArg(args[1], deleteSignature, "to");
                break;
            default:
                throw new ArgumentException($"'delete' method has only the overloads: '{deleteSignature}' or '{deleteAll}', but was called with {args.Length} arguments.");
        }

        var (id, doc) = GetIdAndDocFromArg(document, _timeSeriesSignature);

        string timeSeries = GetStringArg(name, _timeSeriesSignature, "name");

        var count = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_docsCtx, id, timeSeries).Count;
        if (count == 0)
            return ScriptEngineHandle.Undefined;

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
        return ScriptEngineHandle.Undefined;
    }

    private T IncrementTimeSeries(T self, T[] args)
    {
        using var document = self.GetProperty("doc");
        using var name = self.GetProperty("name");

        AssertValidDatabaseContext("timeseries(doc, name).increment");

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
                timestamp = GetTimeSeriesDateArg(args[0], signature, "timestamp");
                valuesArg = args[1];
                break;
            default:
                throw new ArgumentException(
                    $"There is no overload with {args.Length} arguments for this method should be {signature1Args} or {signature2Args}");
        }

        var (id, doc) = GetIdAndDocFromArg(document, _timeSeriesSignature);

        string timeSeries = GetStringArg(name, _timeSeriesSignature, "name");

        double[] valuesBuffer = null;
        try
        {
            GetTimeSeriesValues(valuesArg, ref valuesBuffer, signature, out var values);

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

        return ScriptEngineHandle.Undefined;
    }

    private T AppendTimeSeries(T self, T[] args)
    {
        AssertValidDatabaseContext("timeseries(doc, name).append");

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
                if (/*tagArgument != null && */tagArgument.IsNull == false && tagArgument.IsUndefined == false)
                {
                    var tag = GetStringArg(tagArgument, signature, "tag");
                    lsTag = _jsonCtx.GetLazyString(tag);
                }
                break;
            default:
                throw new ArgumentException($"There is no overload with {args.Length} arguments for this method should be {signature2Args} or {signature3Args}");
        }

        using var document = self.GetProperty("doc");
        using var name = self.GetProperty("name");
        var (id, doc) = GetIdAndDocFromArg(document, _timeSeriesSignature);

        string timeSeries = GetStringArg(name, _timeSeriesSignature, "name");
        var timestamp = GetTimeSeriesDateArg(args[0], signature, "timestamp");

        double[] valuesBuffer = null;
        try
        {
            var valuesArg = args[1];

            GetTimeSeriesValues(valuesArg, ref valuesBuffer, signature, out var values);

            var tss = _database.DocumentsStorage.TimeSeriesStorage;
            var newSeries = tss.Stats.GetStats(_docsCtx, id, timeSeries).Count == 0;

            if (newSeries)
            {
                DocumentTimeSeriesToUpdate ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                DocumentTimeSeriesToUpdate.Add(id);
            }

            var toAppend = new SingleResult
            {
                Values = values,
                Tag = lsTag,
                Timestamp = timestamp,
                Status = TimeSeriesValuesSegment.Live
            };

            tss.AppendTimestamp(
                _docsCtx,
                id,
                CollectionName.GetCollectionName(doc),
                timeSeries,
                new[] { toAppend },
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

        return ScriptEngineHandle.Undefined;
    }

    private DateTime GetTimeSeriesDateArg(T arg, string signature, string argName)
    {
        if (arg.IsDate)
            return arg.AsDate;

        if (arg.IsStringEx == false)
            throw new ArgumentException($"{signature} : {argName} must be of type 'DateInstance' or a DateTime string. {GetTypes(arg)}");

        return TimeSeriesRetriever.ParseDateTime(arg.AsString);
    }

    private void GetTimeSeriesValues(T valuesArg, ref double[] valuesBuffer, string signature, out Memory<double> values)
    {
        if (valuesArg.IsArray)
        {
            valuesBuffer = ArrayPool<double>.Shared.Rent((int)valuesArg.ArrayLength);
            FillDoubleArrayFromJsArray(valuesBuffer, valuesArg, signature);
            values = new Memory<double>(valuesBuffer, 0, (int)valuesArg.ArrayLength);
        }
        else if (valuesArg.IsNumberOrIntEx)
        {
            valuesBuffer = ArrayPool<double>.Shared.Rent(1);
            valuesBuffer[0] = valuesArg.AsDouble;
            values = new Memory<double>(valuesBuffer, 0, 1);
        }
        else
        {
            throw new ArgumentException($"{signature}: The values should be an array but got {GetTypes(valuesArg)}");
        }
    }
    private void FillDoubleArrayFromJsArray(double[] array, T jsArray, string signature)
    {
        int arrayLength = jsArray.ArrayLength;
        for (int i = 0; i < arrayLength; ++i)
        {
            using (var jsItem = jsArray.GetProperty(i))
            {
                if (jsItem.IsNumberOrIntEx == false)
                    throw new ArgumentException($"{signature}: The values argument must be an array of numbers, but got {jsItem.ValueType} key({i}) value({jsItem})");
                array[i] = jsItem.AsDouble;
            }
        }
    }

    private T GetStatsTimeSeries(T self, T[] args)
    {
        using var document = self.GetProperty("doc");
        using var name = self.GetProperty("name");
        var (id, _) = GetIdAndDocFromArg(document, _timeSeriesSignature);

        var timeSeries = GetStringArg(name, _timeSeriesSignature, "name");
        var stats = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_docsCtx, id, timeSeries);

        var tsStats = ScriptEngineHandle.CreateObject();
        tsStats.SetProperty(nameof(stats.Start), ScriptEngineHandle.CreateValue(stats.Start));
        tsStats.SetProperty(nameof(stats.End), ScriptEngineHandle.CreateValue(stats.End));
        tsStats.SetProperty(nameof(stats.Count), ScriptEngineHandle.CreateValue(stats.Count));
        return tsStats;
    }

    private string GetStringArg(T jsArg, string signature, string argName)
    {
        if (jsArg.IsStringEx == false)
            throw new ArgumentException($"{signature}: The '{argName}' argument should be a string, but got {GetTypes(jsArg)}");
        return jsArg.AsString;
    }

    private (string Id, BlittableJsonReaderObject Doc) GetIdAndDocFromArg(T self, string signature)
    {
        if (self.IsObject && self.AsObject() is IBlittableObjectInstance doc)
            return (doc.DocumentId, doc.Blittable);

        if (self.IsStringEx)
        {
            var id = self.AsString;
            var document = _database.DocumentsStorage.Get(_docsCtx, id);
            if (document == null)
                throw new DocumentDoesNotExistException(id, "Cannot operate on a missing document.");

            return (id, document.Data);
        }

        throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself. {GetTypes(self)}");
    }

    public T ExplodeArgs(T self, T[] args)
    {
        if (args.Length != 2)
            throw new InvalidOperationException("Raven_ExplodeArgs(this, args) - must be called with 2 arguments");

        if (args[1].IsObject && args[1].Object is IBlittableObjectInstance boi)
        {
            SetArgs(args, boi);
            return self;
        }
        if (args[1].IsNull || args[1].IsUndefined)
            return self;// noop

        throw new InvalidOperationException("Raven_ExplodeArgs(this, args) second argument must be BlittableObjectInstance");
    }

    protected abstract string GetTypes(T value);
    protected abstract void SetArgs(T[] args, IBlittableObjectInstance boi);
    public T Raven_Min(T self, T[] args)
    {
        GenericSortTwoElementArray(args);
        return args[0].IsNull ? args[1] : args[0];
    }

    public T Raven_Max(T self, T[] args)
    {
        GenericSortTwoElementArray(args);
        return args[1];
    }

    private void GenericSortTwoElementArray(T[] args, [CallerMemberName] string caller = null)
    {
        void Swap()
        {
            var tmp = args[1];
            args[1] = args[0];
            args[0] = tmp;
        }

        // this is basically the same as Math.min / Math.max, but
        // can also be applied to strings, numbers and nulls

        if (args.Length != 2)
            throw new ArgumentException(caller + "must be called with exactly two arguments");

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
                        Swap();// a value is bigger than no value
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
                throw new ArgumentException(caller + " cannot be called on an object");
        }
    }


    public static T ThrowOnLoadDocument(T self, T[] args)
    {
        throw new MissingMethodException("The method LoadDocument was renamed to 'load'");
    }

    public static T ThrowOnDeleteDocument(T self, T[] args)
    {
        throw new MissingMethodException("The method DeleteDocument was renamed to 'del'");
    }

    public static T ThrowOnPutDocument(T self, T[] args)
    {
        throw new MissingMethodException("The method PutDocument was renamed to 'put'");
    }

    private static void ThrowInvalidIncrementCounterArgs(T[] args)
    {
        throw new InvalidOperationException($"There is no overload of method 'incrementCounter' that takes {args.Length} arguments." +
                                            "Supported overloads are : 'incrementCounter(doc, name)' , 'incrementCounter(doc, name, value)'");
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
            _args[1].AsObject() is IBlittableObjectInstance)
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

    public object[] GetTimeSeriesFunctionArgs(string name, T[] args, out string docId, out List<IDisposable> lazyIds)
    {
        var tsFunctionArgs = new object[args.Length + 1];
        docId = null;

        lazyIds = new List<IDisposable>();

        for (var index = 0; index < args.Length; index++)
        {
            if (args[index].Object is IBlittableObjectInstance boi)
            {
                var lazyId = _docsCtx.GetLazyString(boi.DocumentId);
                lazyIds.Add(lazyId);
                tsFunctionArgs[index] = new Document
                {
                    Data = boi.Blittable,
                    Id = lazyId
                };

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
                !(_args[0].Object is IBlittableObjectInstance originalDoc))
                throw new InvalidOperationException($"Failed to invoke time series function '{name}'. Couldn't find the document ID to operate on. " +
                                                    "A Document instance argument was not provided to the time series function or to the ScriptRunner");

            docId = originalDoc.DocumentId;
        }

        if (_args[_args.Length - 1].IsObject == false || !(_args[_args.Length - 1].Object is IBlittableObjectInstance queryParams))
            throw new InvalidOperationException($"Failed to invoke time series function '{name}'. ScriptRunner is missing QueryParameters argument");

        tsFunctionArgs[tsFunctionArgs.Length - 1] = new Document
        {
            Data = queryParams.Blittable
        };

        return tsFunctionArgs;
    }


    public object Translate(JsonOperationContext context, object o)
    {
        return JsUtils.TranslateToJs(context, o);
    }

    public T CreateEmptyObject()
    {
        return ScriptEngineHandle.CreateObject();
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
        throw new NotSupportedException("Unable to translate " + jsValue.ValueType);
    }

    public override string ToString()
    {
        return string.Join(Environment.NewLine, _runner.ScriptsSource);
    }

    protected static void AssertValidId()
    {
        throw new InvalidOperationException("The first parameter to put(id, doc, changeVector) must be a string");
    }

    protected void AssertNotReadOnly()
    {
        if (ReadOnly)
            throw new InvalidOperationException("Cannot make modifications in readonly context");
    }

    protected void AssertValidDatabaseContext(string functionName)
    {
        if (_docsCtx == null)
            throw new InvalidOperationException($"Unable to use `{functionName}` when this instance is not attached to a database operation");
    }

    protected static void ThrowInvalidCounterValue()
    {
        throw new InvalidOperationException("incrementCounter(doc, name, value): 'value' must be a number argument");
    }

    protected static void ThrowInvalidCounterName(string signature)
    {
        throw new InvalidOperationException($"{signature}: 'name' must be a non-empty string argument");
    }

    protected static void ThrowInvalidDocumentArgsType(string signature)
    {
        throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself");
    }

    protected static void ThrowMissingDocument(string id)
    {
        throw new DocumentDoesNotExistException(id, "Cannot operate on counters of a missing document.");
    }

    protected static void ThrowDeleteCounterNameArg()
    {
        throw new InvalidOperationException("deleteCounter(doc, name): 'name' must be a string argument");
    }

    protected static void ThrowInvalidDeleteCounterDocumentArg()
    {
        throw new InvalidOperationException("deleteCounter(doc, name): 'doc' must be a string argument (the document id) or the actual document instance itself");
    }

    protected static void ThrowInvalidDeleteCounterArgs()
    {
        throw new InvalidOperationException("deleteCounter(doc, name) must be called with exactly 2 arguments");
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

        ScriptEngineHandle.ResetCallStack();
        ScriptEngineHandle.ResetConstraints();
    }

    protected abstract void DisposeArgs();

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
