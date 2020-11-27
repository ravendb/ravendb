using System;
using System.Buffers;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Threading;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Results;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Spatial4n.Core.Distance;
using ExpressionType = System.Linq.Expressions.ExpressionType;
using JavaScriptException = Jint.Runtime.JavaScriptException;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunner
    {
        public class Holder
        {
            public ScriptRunner Parent;
            public SingleRun Value;
            public WeakReference<SingleRun> WeakValue;
        }

        private readonly ConcurrentQueue<Holder> _cache = new ConcurrentQueue<Holder>();
        private readonly DocumentDatabase _db;
        private readonly RavenConfiguration _configuration;
        internal readonly bool _enableClr;
        private readonly DateTime _creationTime;
        public readonly List<string> ScriptsSource = new List<string>();

        public int NumberOfCachedScripts => _cache.Count(x =>
            x.Value != null ||
            x.WeakValue?.TryGetTarget(out _) == true);

        internal readonly Dictionary<string, DeclaredFunction> TimeSeriesDeclaration = new Dictionary<string, DeclaredFunction>();

        public long Runs;
        private DateTime _lastRun;

        public string ScriptType { get; internal set; }

        public ScriptRunner(DocumentDatabase db, RavenConfiguration configuration, bool enableClr)
        {
            _db = db;
            _configuration = configuration;
            _enableClr = enableClr;
            _creationTime = DateTime.UtcNow;
        }

        public DynamicJsonValue GetDebugInfo(bool detailed = false)
        {
            var djv = new DynamicJsonValue
            {
                ["Type"] = ScriptType,
                ["CreationTime"] = _creationTime,
                ["LastRun"] = _lastRun,
                ["Runs"] = Runs,
                ["CachedScriptsCount"] = _cache.Count
            };
            if (detailed)
                djv["ScriptsSource"] = ScriptsSource;

            return djv;
        }

        public void AddScript(string script)
        {
            ScriptsSource.Add(script);
        }

        public void AddTimeSeriesDeclaration(DeclaredFunction func)
        {
            TimeSeriesDeclaration.Add(func.Name, func);
        }

        public ReturnRun GetRunner(out SingleRun run)
        {
            _lastRun = DateTime.UtcNow;
            Interlocked.Increment(ref Runs);
            if (_cache.TryDequeue(out var holder) == false)
            {
                holder = new Holder
                {
                    Parent = this
                };
            }

            if (holder.Value == null)
            {
                if (holder.WeakValue != null &&
                    holder.WeakValue.TryGetTarget(out run))
                {
                    holder.Value = run;
                    holder.WeakValue = null;
                }
                else
                {
                    holder.Value = new SingleRun(_db, _configuration, this, ScriptsSource);
                }
            }

            run = holder.Value;

            return new ReturnRun(run, holder);
        }

        public void TryCompileScript(string script)
        {
            try
            {
                var engine = new Engine(options =>
                {
                    options.MaxStatements(1).LimitRecursion(1);
                });
                engine.Execute(script);
            }
            catch (Exception e)
            {
                throw new JavaScriptParseException("Failed to parse:" + Environment.NewLine + script, e);
            }
        }

        public static unsafe DateTime GetDateArg(JsValue arg, string signature, string argName)
        {
            if (arg.IsDate())
                return arg.AsDate().ToDateTime();

            if (arg.IsString() == false)
                ThrowInvalidDateArgument();

            var s = arg.AsString();
            fixed (char* pValue = s)
            {
                var result = LazyStringParser.TryParseDateTime(pValue, s.Length, out DateTime dt, out _);
                if (result != LazyStringParser.Result.DateTime)
                    ThrowInvalidDateArgument();

                return dt;
            }

            void ThrowInvalidDateArgument() =>
                throw new ArgumentException($"{signature} : {argName} must be of type 'DateInstance' or a DateTime string. {GetTypes(arg)}");
        }

        private static DateTime GetTimeSeriesDateArg(JsValue arg, string signature, string argName)
        {
            if (arg.IsDate())
                return arg.AsDate().ToDateTime();

            if (arg.IsString() == false)
                throw new ArgumentException($"{signature} : {argName} must be of type 'DateInstance' or a DateTime string. {GetTypes(arg)}");

            return TimeSeriesRetriever.ParseDateTime(arg.AsString());
        }
        
        private static string GetTypes(JsValue value) => $"JintType({value.Type}) .NETType({value.GetType().Name})";
        
        public class SingleRun
        {
            private readonly DocumentDatabase _database;
            private readonly RavenConfiguration _configuration;

            private readonly ScriptRunner _runner;
            public readonly Engine ScriptEngine;
            private QueryTimingsScope _scope;
            private QueryTimingsScope _loadScope;
            private DocumentsOperationContext _docsCtx;
            private JsonOperationContext _jsonCtx;
            public PatchDebugActions DebugActions;
            public bool DebugMode;
            public List<string> DebugOutput;
            public bool PutOrDeleteCalled;
            public HashSet<string> Includes;
            public HashSet<string> CompareExchangeValueIncludes;
            private HashSet<string> _documentIds;

            public bool ReadOnly
            {
                get => JavaScriptUtils.ReadOnly;
                set => JavaScriptUtils.ReadOnly = value;
            }

            public string OriginalDocumentId;
            public bool RefreshOriginalDocument;
            private readonly ConcurrentLruRegexCache _regexCache = new ConcurrentLruRegexCache(1024);
            public HashSet<string> DocumentCountersToUpdate;
            public HashSet<string> DocumentTimeSeriesToUpdate;
            public JavaScriptUtils JavaScriptUtils;

            private const string _timeSeriesSignature = "timeseries(doc, name)";
            public const string GetMetadataMethod = "getMetadata";

            public SingleRun(DocumentDatabase database, RavenConfiguration configuration, ScriptRunner runner, List<string> scriptsSource)
            {
                _database = database;
                _configuration = configuration;
                _runner = runner;
                ScriptEngine = new Engine(options =>
                {
                    options.LimitRecursion(64)
                        .SetReferencesResolver(_refResolver)
                        .MaxStatements(_configuration.Patching.MaxStepsForScript)
                        .Strict(_configuration.Patching.StrictMode)
                        .AddObjectConverter(new JintGuidConverter())
                        .AddObjectConverter(new JintStringConverter())
                        .AddObjectConverter(new JintEnumConverter())
                        .AddObjectConverter(new JintDateTimeConverter())
                        .AddObjectConverter(new JintTimeSpanConverter())
                        .LocalTimeZone(TimeZoneInfo.Utc);
                });

                JavaScriptUtils = new JavaScriptUtils(_runner, ScriptEngine);
                ScriptEngine.SetValue(GetMetadataMethod, new ClrFunctionInstance(ScriptEngine, GetMetadataMethod, JavaScriptUtils.GetMetadata));
                ScriptEngine.SetValue("id", new ClrFunctionInstance(ScriptEngine, "id", JavaScriptUtils.GetDocumentId));

                ScriptEngine.SetValue("output", new ClrFunctionInstance(ScriptEngine, "output", OutputDebug));

                //console.log
                ObjectInstance consoleObject = new ObjectInstance(ScriptEngine);
                consoleObject.FastAddProperty("log", new ClrFunctionInstance(ScriptEngine, "log", OutputDebug), false, false, false);
                ScriptEngine.SetValue("console", consoleObject);

                //spatial.distance
                ObjectInstance spatialObject = new ObjectInstance(ScriptEngine);
                var spatialFunc = new ClrFunctionInstance(ScriptEngine, "distance", Spatial_Distance);
                spatialObject.FastAddProperty("distance", spatialFunc, false, false, false);
                ScriptEngine.SetValue("spatial", spatialObject);
                ScriptEngine.SetValue("spatial.distance", spatialFunc);

                // includes
                var includeDocumentFunc = new ClrFunctionInstance(ScriptEngine, "include", IncludeDoc);
                ObjectInstance includesObject = new ObjectInstance(ScriptEngine);
                includesObject.FastAddProperty("document", includeDocumentFunc, false, false, false);
                includesObject.FastAddProperty("cmpxchg", new ClrFunctionInstance(ScriptEngine, "cmpxchg", IncludeCompareExchangeValue), false, false, false);
                ScriptEngine.SetValue("includes", includesObject);

                // includes - backward compatibility
                ScriptEngine.SetValue("include", includeDocumentFunc);

                ScriptEngine.SetValue("load", new ClrFunctionInstance(ScriptEngine, "load", LoadDocument));
                ScriptEngine.SetValue("LoadDocument", new ClrFunctionInstance(ScriptEngine, "LoadDocument", ThrowOnLoadDocument));

                ScriptEngine.SetValue("loadPath", new ClrFunctionInstance(ScriptEngine, "loadPath", LoadDocumentByPath));
                ScriptEngine.SetValue("del", new ClrFunctionInstance(ScriptEngine, "del", DeleteDocument));
                ScriptEngine.SetValue("DeleteDocument", new ClrFunctionInstance(ScriptEngine, "DeleteDocument", ThrowOnDeleteDocument));
                ScriptEngine.SetValue("put", new ClrFunctionInstance(ScriptEngine, "put", PutDocument));
                ScriptEngine.SetValue("PutDocument", new ClrFunctionInstance(ScriptEngine, "PutDocument", ThrowOnPutDocument));
                ScriptEngine.SetValue("cmpxchg", new ClrFunctionInstance(ScriptEngine, "cmpxchg", CompareExchange));

                ScriptEngine.SetValue("counter", new ClrFunctionInstance(ScriptEngine, "counter", GetCounter));
                ScriptEngine.SetValue("counterRaw", new ClrFunctionInstance(ScriptEngine, "counterRaw", GetCounterRaw));
                ScriptEngine.SetValue("incrementCounter", new ClrFunctionInstance(ScriptEngine, "incrementCounter", IncrementCounter));
                ScriptEngine.SetValue("deleteCounter", new ClrFunctionInstance(ScriptEngine, "deleteCounter", DeleteCounter));

                ScriptEngine.SetValue("lastModified", new ClrFunctionInstance(ScriptEngine, "lastModified", GetLastModified));

                ScriptEngine.SetValue("startsWith", new ClrFunctionInstance(ScriptEngine, "startsWith", StartsWith));
                ScriptEngine.SetValue("endsWith", new ClrFunctionInstance(ScriptEngine, "endsWith", EndsWith));
                ScriptEngine.SetValue("regex", new ClrFunctionInstance(ScriptEngine, "regex", Regex));

                ScriptEngine.SetValue("Raven_ExplodeArgs", new ClrFunctionInstance(ScriptEngine, "Raven_ExplodeArgs", ExplodeArgs));
                ScriptEngine.SetValue("Raven_Min", new ClrFunctionInstance(ScriptEngine, "Raven_Min", Raven_Min));
                ScriptEngine.SetValue("Raven_Max", new ClrFunctionInstance(ScriptEngine, "Raven_Max", Raven_Max));

                ScriptEngine.SetValue("convertJsTimeToTimeSpanString", new ClrFunctionInstance(ScriptEngine, "convertJsTimeToTimeSpanString", ConvertJsTimeToTimeSpanString));
                ScriptEngine.SetValue("convertToTimeSpanString", new ClrFunctionInstance(ScriptEngine, "convertToTimeSpanString", ConvertToTimeSpanString));
                ScriptEngine.SetValue("compareDates", new ClrFunctionInstance(ScriptEngine, "compareDates", CompareDates));

                ScriptEngine.SetValue("toStringWithFormat", new ClrFunctionInstance(ScriptEngine, "toStringWithFormat", ToStringWithFormat));

                ScriptEngine.SetValue("scalarToRawString", new ClrFunctionInstance(ScriptEngine, "scalarToRawString", ScalarToRawString));

                //TimeSeries
                ScriptEngine.SetValue("timeseries", new ClrFunctionInstance(ScriptEngine, "timeseries", TimeSeries));
                ScriptEngine.Execute(ScriptRunnerCache.PolyfillJs);

                foreach (var script in scriptsSource)
                {
                    try
                    {
                        ScriptEngine.Execute(script);
                    }
                    catch (Exception e)
                    {
                        throw new JavaScriptParseException("Failed to parse: " + Environment.NewLine + script, e);
                    }
                }

                foreach (var ts in runner.TimeSeriesDeclaration)
                {
                    ScriptEngine.SetValue(ts.Key, NamedInvokeTimeSeriesFunction(ts.Key));
                }
            }

            private (string Id, BlittableJsonReaderObject Doc) GetIdAndDocFromArg(JsValue docArg, string signature)
            {
                if (docArg.IsObject() && docArg.AsObject() is BlittableObjectInstance doc)
                    return (doc.DocumentId, doc.Blittable);

                if (docArg.IsString())
                {
                    var id = docArg.AsString();
                    var document = _database.DocumentsStorage.Get(_docsCtx, id);
                    if (document == null)
                        throw new DocumentDoesNotExistException(id, "Cannot operate on a missing document.");

                    return (id, document.Data);
                }

                throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself. {GetTypes(docArg)}");
            }

            private string GetIdFromArg(JsValue docArg, string signature)
            {
                if (docArg.IsObject() && docArg.AsObject() is BlittableObjectInstance doc)
                    return doc.DocumentId;

                if (docArg.IsString())
                {
                    var id = docArg.AsString();
                    return id;
                }

                throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself. {GetTypes(docArg)}");
            }

            private static string GetStringArg(JsValue jsArg, string signature, string argName)
            {
                if (jsArg.IsString() == false)
                    throw new ArgumentException($"{signature}: The '{argName}' argument should be a string, but got {GetTypes(jsArg)}");
                return jsArg.AsString();
            }

            private void FillDoubleArrayFromJsArray(double[] array, ArrayInstance jsArray, string signature)
            {
                var i = 0;
                foreach (var (key, value) in jsArray.GetOwnPropertiesWithoutLength())
                {
                    if (value.Value.IsNumber() == false)
                        throw new ArgumentException($"{signature}: The values argument must be an array of numbers, but got {GetTypes(value.Value)} key({key}) value({value})");
                    array[i] = value.Value.AsNumber();
                    ++i;
                }
            }

            private JsValue TimeSeries(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext(_timeSeriesSignature);

                if (args.Length != 2)
                    throw new ArgumentException($"{_timeSeriesSignature}: This method requires 2 arguments but was called with {args.Length}");

                var append = new ClrFunctionInstance(ScriptEngine, "append", (thisObj, values) =>
                    AppendTimeSeries(thisObj.Get("doc"), thisObj.Get("name"), values));

                var delete = new ClrFunctionInstance(ScriptEngine, "delete", (thisObj, values) =>
                    DeleteRangeTimeSeries(thisObj.Get("doc"), thisObj.Get("name"), values));

                var get = new ClrFunctionInstance(ScriptEngine, "get", (thisObj, values) =>
                    GetRangeTimeSeries(thisObj.Get("doc"), thisObj.Get("name"), values));

                var getStats = new ClrFunctionInstance(ScriptEngine, "getStats", (thisObj, values) =>
                    GetStatsTimeSeries(thisObj.Get("doc"), thisObj.Get("name"), values));

                var obj = new ObjectInstance(ScriptEngine);
                obj.Set("append", append);
                obj.Set("delete", delete);
                obj.Set("get", get);
                obj.Set("doc", args[0]);
                obj.Set("name", args[1]);
                obj.Set("getStats", getStats);

                return obj;
            }

            private JsValue GetStatsTimeSeries(JsValue document, JsValue name, JsValue[] args)
            {
                var (id, doc) = GetIdAndDocFromArg(document, _timeSeriesSignature);

                string timeSeries = GetStringArg(name, _timeSeriesSignature, "name");
                var stats = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_docsCtx, id, timeSeries);

                var tsStats = new ObjectInstance(ScriptEngine);
                tsStats.Set(nameof(stats.Start), ScriptEngine.Date.Construct(stats.Start));
                tsStats.Set(nameof(stats.End), ScriptEngine.Date.Construct(stats.End));
                tsStats.Set(nameof(stats.Count), stats.Count);

                return tsStats;
            }

            private JsValue AppendTimeSeries(JsValue document, JsValue name, JsValue[] args)
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
                        if (tagArgument != null && tagArgument.IsNull() == false && tagArgument.IsUndefined() == false)
                        {
                            var tag = GetStringArg(tagArgument, signature, "tag");
                            lsTag = _jsonCtx.GetLazyString(tag);
                        }
                        break;
                    default:
                        throw new ArgumentException($"There is no overload with {args.Length} arguments for this method should be {signature2Args} or {signature3Args}");
                }

                var (id, doc) = GetIdAndDocFromArg(document, _timeSeriesSignature);

                string timeSeries = GetStringArg(name, _timeSeriesSignature, "name");
                var timestamp = GetTimeSeriesDateArg(args[0], signature, "timestamp");

                double[] valuesBuffer = null;
                try
                {
                    var valuesArg = args[1];
                    Memory<double> values;
                    if (valuesArg.IsArray())
                    {
                        var jsValues = valuesArg.AsArray();
                        valuesBuffer = ArrayPool<double>.Shared.Rent((int)jsValues.Length);
                        FillDoubleArrayFromJsArray(valuesBuffer, jsValues, signature);
                        values = new Memory<double>(valuesBuffer, 0, (int)jsValues.Length);
                    }
                    else if (valuesArg.IsNumber())
                    {
                        valuesBuffer = ArrayPool<double>.Shared.Rent(1);
                        valuesBuffer[0] = valuesArg.AsNumber();
                        values = new Memory<double>(valuesBuffer, 0, 1);
                    }
                    else
                    {
                        throw new ArgumentException($"{signature}: The values should be an array but got {GetTypes(valuesArg)}");
                    }

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
                        addNewNameToMetadata: false);

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

                return Undefined.Instance;
            }

            private JsValue DeleteRangeTimeSeries(JsValue document, JsValue name, JsValue[] args)
            {
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
                    return JsValue.Undefined;

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

                return JsValue.Undefined;
            }

            private JsValue GetRangeTimeSeries(JsValue document, JsValue name, JsValue[] args)
            {
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

                var entries = new List<JsValue>();
                foreach (var singleResult in reader.AllValues())
                {
                    Span<double> valuesSpan = singleResult.Values.Span;
                    var v = new JsValue[valuesSpan.Length];
                    for (int i = 0; i < valuesSpan.Length; i++)
                    {
                        v[i] = valuesSpan[i];
                    }
                    var jsValues = new ArrayInstance(ScriptEngine);
                    jsValues.FastAddProperty("length", 0, true, false, false);
                    ScriptEngine.Array.PrototypeObject.Push(jsValues, v);

                    var entry = new ObjectInstance(ScriptEngine);
                    entry.Set(nameof(TimeSeriesEntry.Timestamp), singleResult.Timestamp.GetDefaultRavenFormat(isUtc: true));
                    entry.Set(nameof(TimeSeriesEntry.Tag), singleResult.Tag?.ToString());
                    entry.Set(nameof(TimeSeriesEntry.Values), jsValues);
                    entry.Set(nameof(TimeSeriesEntry.IsRollup), singleResult.Type == SingleResultType.RolledUp);
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

                return ScriptEngine.Array.Construct(entries.ToArray());
            }

            private void GenericSortTwoElementArray(JsValue[] args, [CallerMemberName] string caller = null)
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

                switch (args[0].Type)
                {
                    case Jint.Runtime.Types.None:
                    case Jint.Runtime.Types.Undefined:
                    case Jint.Runtime.Types.Null:
                        // null sorts lowers, so that is fine (either the other one is null or
                        // already higher than us).
                        break;
                    case Jint.Runtime.Types.Boolean:
                    case Jint.Runtime.Types.Number:
                        var a = Jint.Runtime.TypeConverter.ToNumber(args[0]);
                        var b = Jint.Runtime.TypeConverter.ToNumber(args[1]);
                        if (a > b)
                            Swap();
                        break;
                    case Jint.Runtime.Types.String:
                        switch (args[1].Type)
                        {
                            case Jint.Runtime.Types.None:
                            case Jint.Runtime.Types.Undefined:
                            case Jint.Runtime.Types.Null:
                                Swap();// a value is bigger than no value
                                break;
                            case Jint.Runtime.Types.Boolean:
                            case Jint.Runtime.Types.Number:
                                // if the string value is a number that is smaller than
                                // the numeric value, because Math.min(true, "-2") works :-(
                                if (double.TryParse(args[0].AsString(), out double d) == false ||
                                    d > Jint.Runtime.TypeConverter.ToNumber(args[1]))
                                {
                                    Swap();
                                }
                                break;
                            case Jint.Runtime.Types.String:
                                if (string.Compare(args[0].AsString(), args[1].AsString()) > 0)
                                    Swap();
                                break;
                        }
                        break;
                    case Jint.Runtime.Types.Object:
                        throw new ArgumentException(caller + " cannot be called on an object");
                }
            }

            private JsValue Raven_Max(JsValue self, JsValue[] args)
            {
                GenericSortTwoElementArray(args);
                return args[1];
            }

            private JsValue Raven_Min(JsValue self, JsValue[] args)
            {
                GenericSortTwoElementArray(args);
                return args[0];
            }

            private JsValue IncludeDoc(JsValue self, JsValue[] args)
            {
                if (args.Length != 1)
                    throw new InvalidOperationException("include(id) must be called with a single argument");

                if (args[0].IsNull() || args[0].IsUndefined())
                    return args[0];

                if (args[0].IsArray())// recursive call ourselves
                {
                    var array = args[0].AsArray();
                    foreach (var pair in array.GetOwnPropertiesWithoutLength())
                    {
                        args[0] = pair.Value.Value;
                        if (args[0].IsString())
                            IncludeDoc(self, args);
                    }
                    return self;
                }

                if (args[0].IsString() == false)
                    throw new InvalidOperationException("include(doc) must be called with an string or string array argument");

                var id = args[0].AsString();

                if (Includes == null)
                    Includes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                Includes.Add(id);

                return self;
            }

            private JsValue IncludeCompareExchangeValue(JsValue self, JsValue[] args)
            {
                if (args.Length != 1)
                    throw new InvalidOperationException("includes.cmpxchg(key) must be called with a single argument");

                if (args[0].IsNull() || args[0].IsUndefined())
                    return self;

                if (args[0].IsArray())// recursive call ourselves
                {
                    var array = args[0].AsArray();
                    foreach (var pair in array.GetOwnPropertiesWithoutLength())
                    {
                        args[0] = pair.Value.Value;
                        if (args[0].IsString())
                            IncludeCompareExchangeValue(self, args);
                    }
                    return self;
                }

                if (args[0].IsString() == false)
                    throw new InvalidOperationException("includes.cmpxchg(key) must be called with an string or string array argument");

                var key = args[0].AsString();

                if (CompareExchangeValueIncludes == null)
                    CompareExchangeValueIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                CompareExchangeValueIncludes.Add(key);

                return self;
            }

            public override string ToString()
            {
                return string.Join(Environment.NewLine, _runner.ScriptsSource);
            }

            private static JsValue GetLastModified(JsValue self, JsValue[] args)
            {
                if (args.Length != 1)
                    throw new InvalidOperationException("lastModified(doc) must be called with a single argument");

                if (args[0].IsNull() || args[0].IsUndefined())
                    return args[0];

                if (args[0].IsObject() == false)
                    throw new InvalidOperationException("lastModified(doc) must be called with an object argument");

                if (args[0].AsObject() is BlittableObjectInstance doc)
                {
                    if (doc.LastModified == null)
                        return Undefined.Instance;

                    // we use UTC because last modified is in UTC
                    var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                    var jsTime = doc.LastModified.Value.Subtract(epoch)
                        .TotalMilliseconds;
                    return jsTime;
                }
                return Undefined.Instance;
            }

            private JsValue Spatial_Distance(JsValue self, JsValue[] args)
            {
                if (args.Length < 4 && args.Length > 5)
                    throw new ArgumentException("Called with expected number of arguments, expected: spatial.distance(lat1, lng1, lat2, lng2, kilometers | miles | cartesian)");

                for (int i = 0; i < 4; i++)
                {
                    if (args[i].IsNumber() == false)
                        return Undefined.Instance;
                }

                var lat1 = args[0].AsNumber();
                var lng1 = args[1].AsNumber();
                var lat2 = args[2].AsNumber();
                var lng2 = args[3].AsNumber();

                var units = SpatialUnits.Kilometers;
                if (args.Length > 4 && args[4].IsString())
                {
                    if (string.Equals("cartesian", args[4].AsString(), StringComparison.OrdinalIgnoreCase))
                        return SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator.CartesianDistance(lat1, lng1, lat2, lng2);

                    if (Enum.TryParse(args[4].AsString(), ignoreCase: true, out units) == false)
                        throw new ArgumentException("Unable to parse units " + args[5] + ", expected: 'kilomoters' or 'miles'");
                }

                var result = SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator.HaverstineDistanceInMiles(lat1, lng1, lat2, lng2);
                if (units == SpatialUnits.Kilometers)
                    result *= DistanceUtils.MILES_TO_KM;

                return result;
            }

            private JsValue OutputDebug(JsValue self, JsValue[] args)
            {
                if (DebugMode == false)
                    return self;

                var obj = args[0];

                DebugOutput.Add(GetDebugValue(obj, false));
                return self;
            }

            private string GetDebugValue(JsValue obj, bool recursive)
            {
                if (obj.IsString())
                {
                    var debugValue = obj.ToString();
                    return recursive ? '"' + debugValue + '"' : debugValue;
                }
                if (obj.IsArray())
                {
                    var sb = new StringBuilder("[");
                    var array = obj.AsArray();
                    var jsValue = (int)array.Get("length").AsNumber();
                    for (var i = 0; i < jsValue; i++)
                    {
                        if (i != 0)
                            sb.Append(",");
                        sb.Append(GetDebugValue(array.Get(i.ToString()), true));
                    }
                    sb.Append("]");
                    return sb.ToString();
                }
                if (obj.IsObject())
                {
                    var result = new ScriptRunnerResult(this, obj);
                    using (var jsonObj = result.TranslateToObject(_jsonCtx))
                    {
                        return jsonObj.ToString();
                    }
                }
                if (obj.IsBoolean())
                    return obj.AsBoolean().ToString();
                if (obj.IsNumber())
                    return obj.AsNumber().ToString(CultureInfo.InvariantCulture);
                if (obj.IsNull())
                    return "null";
                if (obj.IsUndefined())
                    return "undefined";
                return obj.ToString();
            }

            public JsValue ExplodeArgs(JsValue self, JsValue[] args)
            {
                if (args.Length != 2)
                    throw new InvalidOperationException("Raven_ExplodeArgs(this, args) - must be called with 2 arguments");
                if (args[1].IsObject() && args[1].AsObject() is BlittableObjectInstance boi)
                {
                    _refResolver.ExplodeArgsOn(args[0], boi);
                    return self;
                }
                if (args[1].IsNull() || args[1].IsUndefined())
                    return self;// noop
                throw new InvalidOperationException("Raven_ExplodeArgs(this, args) second argument must be BlittableObjectInstance");
            }

            public JsValue PutDocument(JsValue self, JsValue[] args)
            {
                string changeVector = null;

                if (args.Length != 2 && args.Length != 3)
                    throw new InvalidOperationException("put(id, doc, changeVector) must be called with called with 2 or 3 arguments only");
                AssertValidDatabaseContext("put document");
                AssertNotReadOnly();
                if (args[0].IsString() == false && args[0].IsNull() == false && args[0].IsUndefined() == false)
                    AssertValidId();

                var id = args[0].IsNull() || args[0].IsUndefined() ? null : args[0].AsString();

                if (args[1].IsObject() == false)
                    throw new InvalidOperationException(
                        $"Created document must be a valid object which is not null or empty. Document ID: '{id}'.");

                PutOrDeleteCalled = true;

                if (args.Length == 3)
                    if (args[2].IsString())
                        changeVector = args[2].AsString();
                    else if (args[2].IsNull() == false && args[0].IsUndefined() == false)
                        throw new InvalidOperationException(
                            $"The change vector must be a string or null. Document ID: '{id}'.");

                BlittableJsonReaderObject reader = null;
                try
                {
                    reader = JsBlittableBridge.Translate(_jsonCtx, ScriptEngine, args[1].AsObject(), usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);

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

                    return put.Id;
                }
                finally
                {
                    if (DebugMode == false)
                        reader?.Dispose();
                }
            }

            private static void AssertValidId()
            {
                throw new InvalidOperationException("The first parameter to put(id, doc, changeVector) must be a string");
            }

            public JsValue DeleteDocument(JsValue self, JsValue[] args)
            {
                if (args.Length != 1 && args.Length != 2)
                    throw new InvalidOperationException("delete(id, changeVector) must be called with at least one parameter");

                if (args[0].IsString() == false)
                    throw new InvalidOperationException("delete(id, changeVector) id argument must be a string");

                var id = args[0].AsString();
                string changeVector = null;

                if (args.Length == 2 && args[1].IsString())
                    changeVector = args[1].AsString();

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

                return result != null;
            }

            private void AssertNotReadOnly()
            {
                if (ReadOnly)
                    throw new InvalidOperationException("Cannot make modifications in readonly context");
            }

            private void AssertValidDatabaseContext(string functionName)
            {
                if (_docsCtx == null)
                    throw new InvalidOperationException($"Unable to use `{functionName}` when this instance is not attached to a database operation");
            }

            private JsValue LoadDocumentByPath(JsValue self, JsValue[] args)
            {
                using (_loadScope = _loadScope?.Start() ?? _scope?.For(nameof(QueryTimingsScope.Names.Load)))
                {
                    AssertValidDatabaseContext("loadPath");

                    if (args.Length != 2 ||
                        (args[0].IsNull() == false && args[0].IsUndefined() == false && args[0].IsObject() == false)
                        || args[1].IsString() == false)
                        throw new InvalidOperationException("loadPath(doc, path) must be called with a document and path");

                    if (args[0].IsNull() || args[1].IsUndefined())
                        return args[0];

                    if (args[0].AsObject() is BlittableObjectInstance b)
                    {
                        var path = args[1].AsString();
                        if (_documentIds == null)
                            _documentIds = new HashSet<string>();

                        _documentIds.Clear();
                        IncludeUtil.GetDocIdFromInclude(b.Blittable, path, _documentIds, _database.IdentityPartsSeparator);
                        if (path.IndexOf("[]", StringComparison.InvariantCulture) != -1) // array
                            return JsValue.FromObject(ScriptEngine, _documentIds.Select(LoadDocumentInternal).ToList());
                        if (_documentIds.Count == 0)
                            return JsValue.Null;

                        return LoadDocumentInternal(_documentIds.First());
                    }

                    throw new InvalidOperationException("loadPath(doc, path) must be called with a valid document instance, but got a JS object instead");
                }
            }

            private JsValue CompareExchange(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext("cmpxchg");

                if (args.Length != 1 && args.Length != 2 || args[0].IsString() == false)
                    throw new InvalidOperationException("cmpxchg(key) must be called with a single string argument");

                return CmpXchangeInternal(CompareExchangeKey.GetStorageKey(_database.Name, args[0].AsString()));
            }

            private JsValue LoadDocument(JsValue self, JsValue[] args)
            {
                using (_loadScope = _loadScope?.Start() ?? _scope?.For(nameof(QueryTimingsScope.Names.Load)))
                {
                    AssertValidDatabaseContext("load");

                    if (args.Length != 1)
                        throw new InvalidOperationException($"load(id | ids) must be called with a single string argument");

                    if (args[0].IsNull() || args[0].IsUndefined())
                        return args[0];

                    if (args[0].IsArray())
                    {
                        var results = (ArrayInstance)ScriptEngine.Array.Construct(Array.Empty<JsValue>());
                        var arrayInstance = args[0].AsArray();
                        foreach (var kvp in arrayInstance.GetOwnPropertiesWithoutLength())
                        {
                            if (kvp.Value.Value.IsString() == false)
                                throw new InvalidOperationException("load(ids) must be called with a array of strings, but got " + kvp.Value.Value.Type + " - " + kvp.Value.Value);
                            var result = LoadDocumentInternal(kvp.Value.Value.AsString());
                            ScriptEngine.Array.PrototypeObject.Push(results, new[] { result });
                        }
                        return results;
                    }

                    if (args[0].IsString() == false)
                        throw new InvalidOperationException("load(id | ids) must be called with a single string or array argument");

                    return LoadDocumentInternal(args[0].AsString());
                }
            }

            private JsValue GetCounter(JsValue self, JsValue[] args)
            {
                return GetCounterInternal(args);
            }

            private JsValue GetCounterRaw(JsValue self, JsValue[] args)
            {
                return GetCounterInternal(args, true);
            }

            private JsValue GetCounterInternal(JsValue[] args, bool raw = false)
            {
                var signature = raw ? "counterRaw(doc, name)" : "counter(doc, name)";
                AssertValidDatabaseContext(signature);

                if (args.Length != 2)
                    throw new InvalidOperationException($"{signature} must be called with exactly 2 arguments");

                string id;
                if (args[0].IsObject() && args[0].AsObject() is BlittableObjectInstance doc)
                {
                    id = doc.DocumentId;
                }
                else if (args[0].IsString())
                {
                    id = args[0].AsString();
                }
                else
                {
                    throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself");
                }

                if (args[1].IsString() == false)
                {
                    throw new InvalidOperationException($"{signature}: 'name' must be a string argument");
                }

                var name = args[1].AsString();
                if (string.IsNullOrEmpty(id) || string.IsNullOrEmpty(name))
                {
                    return JsValue.Undefined;
                }

                if (raw == false)
                {
                    var counterValue = _database.DocumentsStorage.CountersStorage.GetCounterValue(_docsCtx, id, name)?.Value ?? JsValue.Null;

                    if (DebugMode)
                    {
                        DebugActions.GetCounter.Add(new DynamicJsonValue
                        {
                            ["Name"] = name,
                            ["Value"] = counterValue.ToString(),
                            ["Exists"] = counterValue != JsValue.Null
                        });
                    }

                    return counterValue;
                }

                var rawValues = new ObjectInstance(ScriptEngine);
                foreach (var partialValue in _database.DocumentsStorage.CountersStorage.GetCounterPartialValues(_docsCtx, id, name))
                {
                    rawValues.FastAddProperty(partialValue.ChangeVector, partialValue.PartialValue, true, false, false);
                }

                return rawValues;
            }

            private JsValue IncrementCounter(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext("incrementCounter");

                if (args.Length < 2 || args.Length > 3)
                {
                    ThrowInvalidIncrementCounterArgs(args);
                }

                var signature = args.Length == 2 ? "incrementCounter(doc, name)" : "incrementCounter(doc, name, value)";

                BlittableJsonReaderObject docBlittable = null;
                string id = null;

                if (args[0].IsObject() && args[0].AsObject() is BlittableObjectInstance doc)
                {
                    id = doc.DocumentId;
                    docBlittable = doc.Blittable;
                }
                else if (args[0].IsString())
                {
                    id = args[0].AsString();
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

                if (args[1].IsString() == false)
                    ThrowInvalidCounterName(signature);

                var name = args[1].AsString();
                if (string.IsNullOrWhiteSpace(name))
                    ThrowInvalidCounterName(signature);

                double value = 1;
                if (args.Length == 3)
                {
                    if (args[2].IsNumber() == false)
                        ThrowInvalidCounterValue();
                    value = args[2].AsNumber();
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

                return JsBoolean.True;
            }

            private JsValue DeleteCounter(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext("deleteCounter");

                if (args.Length != 2)
                {
                    ThrowInvalidDeleteCounterArgs();
                }

                string id = null;
                BlittableJsonReaderObject docBlittable = null;

                if (args[0].IsObject() && args[0].AsObject() is BlittableObjectInstance doc)
                {
                    id = doc.DocumentId;
                    docBlittable = doc.Blittable;
                }
                else if (args[0].IsString())
                {
                    id = args[0].AsString();
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

                if (args[1].IsString() == false)
                {
                    ThrowDeleteCounterNameArg();
                }

                var name = args[1].AsString();
                _database.DocumentsStorage.CountersStorage.DeleteCounter(_docsCtx, id, CollectionName.GetCollectionName(docBlittable), name);

                DocumentCountersToUpdate ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                DocumentCountersToUpdate.Add(id);

                if (DebugMode)
                {
                    DebugActions.DeleteCounter.Add(name);
                }

                return JsBoolean.True;
            }

            private ClrFunctionInstance NamedInvokeTimeSeriesFunction(string name)
            {
                return new ClrFunctionInstance(ScriptEngine, name,
                    (self, args) => InvokeTimeSeriesFunction(name, args));
            }

            private JsValue InvokeTimeSeriesFunction(string name, JsValue[] args)
            {
                AssertValidDatabaseContext("InvokeTimeSeriesFunction");

                if (_runner.TimeSeriesDeclaration.TryGetValue(name, out var func) == false)
                    throw new InvalidOperationException($"Failed to invoke time series function. Unknown time series name '{name}'.");

                object[] tsFunctionArgs = GetTimeSeriesFunctionArgs(name, args, out string docId, out var lazyIds);

                var queryParams = ((Document)tsFunctionArgs[tsFunctionArgs.Length - 1]).Data;

                var retriever = new TimeSeriesRetriever(_docsCtx, queryParams, null);

                var streamableResults = retriever.InvokeTimeSeriesFunction(func, docId, tsFunctionArgs, out var type);
                var result = retriever.MaterializeResults(streamableResults, type, addProjectionToResult: false, fromStudio: false);

                foreach (var id in lazyIds)
                {
                    id?.Dispose();
                }

                return JavaScriptUtils.TranslateToJs(ScriptEngine, _jsonCtx, result);
            }

            private object[] GetTimeSeriesFunctionArgs(string name, JsValue[] args, out string docId, out List<IDisposable> lazyIds)
            {
                var tsFunctionArgs = new object[args.Length + 1];
                docId = null;

                lazyIds = new List<IDisposable>();

                for (var index = 0; index < args.Length; index++)
                {
                    if (args[index].IsObject() && args[index].AsObject() is BlittableObjectInstance boi)
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
                    if (_args[0].IsObject() == false ||
                        !(_args[0].AsObject() is BlittableObjectInstance originalDoc))
                        throw new InvalidOperationException($"Failed to invoke time series function '{name}'. Couldn't find the document ID to operate on. " +
                                                            "A Document instance argument was not provided to the time series function or to the ScriptRunner");

                    docId = originalDoc.DocumentId;
                }

                if (_args[_args.Length - 1].IsObject() == false || !(_args[_args.Length - 1].AsObject() is BlittableObjectInstance queryParams))
                    throw new InvalidOperationException($"Failed to invoke time series function '{name}'. ScriptRunner is missing QueryParameters argument");

                tsFunctionArgs[tsFunctionArgs.Length - 1] = new Document
                {
                    Data = queryParams.Blittable
                };

                return tsFunctionArgs;
            }

            private static void ThrowInvalidIncrementCounterArgs(JsValue[] args)
            {
                throw new InvalidOperationException($"There is no overload of method 'incrementCounter' that takes {args.Length} arguments." +
                                                    "Supported overloads are : 'incrementCounter(doc, name)' , 'incrementCounter(doc, name, value)'");
            }

            private static void ThrowInvalidCounterValue()
            {
                throw new InvalidOperationException("incrementCounter(doc, name, value): 'value' must be a number argument");
            }

            private static void ThrowInvalidCounterName(string signature)
            {
                throw new InvalidOperationException($"{signature}: 'name' must be a non-empty string argument");
            }

            private static void ThrowInvalidDocumentArgsType(string signature)
            {
                throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself");
            }

            private static void ThrowMissingDocument(string id)
            {
                throw new DocumentDoesNotExistException(id, "Cannot operate on counters of a missing document.");
            }

            private static void ThrowDeleteCounterNameArg()
            {
                throw new InvalidOperationException("deleteCounter(doc, name): 'name' must be a string argument");
            }

            private static void ThrowInvalidDeleteCounterDocumentArg()
            {
                throw new InvalidOperationException("deleteCounter(doc, name): 'doc' must be a string argument (the document id) or the actual document instance itself");
            }

            private static void ThrowInvalidDeleteCounterArgs()
            {
                throw new InvalidOperationException("deleteCounter(doc, name) must be called with exactly 2 arguments");
            }

            private static JsValue ThrowOnLoadDocument(JsValue self, JsValue[] args)
            {
                throw new MissingMethodException("The method LoadDocument was renamed to 'load'");
            }

            private static JsValue ThrowOnPutDocument(JsValue self, JsValue[] args)
            {
                throw new MissingMethodException("The method PutDocument was renamed to 'put'");
            }

            private static JsValue ThrowOnDeleteDocument(JsValue self, JsValue[] args)
            {
                throw new MissingMethodException("The method DeleteDocument was renamed to 'del'");
            }

            private static JsValue ConvertJsTimeToTimeSpanString(JsValue self, JsValue[] args)
            {
                if (args.Length != 1 || args[0].IsNumber() == false)
                    throw new InvalidOperationException("convertJsTimeToTimeSpanString(ticks) must be called with a single long argument");

                var ticks = Convert.ToInt64(args[0].AsNumber()) * 10000;

                var asTimeSpan = new TimeSpan(ticks);

                return asTimeSpan.ToString();
            }

            private static JsValue ConvertToTimeSpanString(JsValue self, JsValue[] args)
            {
                if (args.Length == 1)
                {
                    if (args[0].IsNumber() == false)
                        throw new InvalidOperationException("convertToTimeSpanString(ticks) must be called with a single long argument");

                    var ticks = Convert.ToInt64(args[0].AsNumber());
                    var asTimeSpan = new TimeSpan(ticks);
                    return asTimeSpan.ToString();
                }

                if (args.Length == 3)
                {
                    if (args[0].IsNumber() == false || args[1].IsNumber() == false || args[2].IsNumber() == false)
                        throw new InvalidOperationException("convertToTimeSpanString(hours, minutes, seconds) must be called with integer values");

                    var hours = Convert.ToInt32(args[0].AsNumber());
                    var minutes = Convert.ToInt32(args[1].AsNumber());
                    var seconds = Convert.ToInt32(args[2].AsNumber());

                    var asTimeSpan = new TimeSpan(hours, minutes, seconds);
                    return asTimeSpan.ToString();
                }

                if (args.Length == 4)
                {
                    if (args[0].IsNumber() == false || args[1].IsNumber() == false || args[2].IsNumber() == false || args[3].IsNumber() == false)
                        throw new InvalidOperationException("convertToTimeSpanString(days, hours, minutes, seconds) must be called with integer values");

                    var days = Convert.ToInt32(args[0].AsNumber());
                    var hours = Convert.ToInt32(args[1].AsNumber());
                    var minutes = Convert.ToInt32(args[2].AsNumber());
                    var seconds = Convert.ToInt32(args[3].AsNumber());

                    var asTimeSpan = new TimeSpan(days, hours, minutes, seconds);
                    return asTimeSpan.ToString();
                }

                if (args.Length == 5)
                {
                    if (args[0].IsNumber() == false || args[1].IsNumber() == false || args[2].IsNumber() == false || args[3].IsNumber() == false || args[4].IsNumber() == false)
                        throw new InvalidOperationException("convertToTimeSpanString(days, hours, minutes, seconds, milliseconds) must be called with integer values");

                    var days = Convert.ToInt32(args[0].AsNumber());
                    var hours = Convert.ToInt32(args[1].AsNumber());
                    var minutes = Convert.ToInt32(args[2].AsNumber());
                    var seconds = Convert.ToInt32(args[3].AsNumber());
                    var milliseconds = Convert.ToInt32(args[4].AsNumber());

                    var asTimeSpan = new TimeSpan(days, hours, minutes, seconds, milliseconds);
                    return asTimeSpan.ToString();
                }

                throw new InvalidOperationException("supported overloads are: " +
                                                    "convertToTimeSpanString(ticks), " +
                                                    "convertToTimeSpanString(hours, minutes, seconds), " +
                                                    "convertToTimeSpanString(days, hours, minutes, seconds), " +
                                                    "convertToTimeSpanString(days, hours, minutes, seconds, milliseconds)");
            }

            private static JsValue CompareDates(JsValue self, JsValue[] args)
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
                else if (args[2].IsString() == false ||
                         Enum.TryParse(args[2].AsString(), out binaryOperationType) == false)
                {
                    throw new InvalidOperationException("compareDates(date1, date2, operationType) : 'operationType' must be a string argument representing a valid 'ExpressionType'");
                }

                dynamic date1, date2;
                if ((binaryOperationType == ExpressionType.Equal ||
                     binaryOperationType == ExpressionType.NotEqual) &&
                    args[0].IsString() && args[1].IsString())
                {
                    date1 = args[0].AsString();
                    date2 = args[1].AsString();
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
                        return (date1 - date2).ToString();
                    case ExpressionType.GreaterThan:
                        return date1 > date2;
                    case ExpressionType.GreaterThanOrEqual:
                        return date1 >= date2;
                    case ExpressionType.LessThan:
                        return date1 < date2;
                    case ExpressionType.LessThanOrEqual:
                        return date1 <= date2;
                    case ExpressionType.Equal:
                        return date1 == date2;
                    case ExpressionType.NotEqual:
                        return date1 != date2;
                    default:
                        throw new InvalidOperationException($"compareDates(date1, date2, binaryOp) : unsupported binary operation '{binaryOperationType}'");
                }
            }

            private static unsafe JsValue ToStringWithFormat(JsValue self, JsValue[] args)
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
                    if (args[i].IsString() == false)
                    {
                        throw new InvalidOperationException("toStringWithFormat : 'format' and 'culture' must be string arguments");
                    }

                    var arg = args[i].AsString();
                    if (CultureHelper.Cultures.TryGetValue(arg, out var culture))
                    {
                        cultureInfo = culture;
                        continue;
                    }

                    format = arg;
                }

                if (args[0].IsDate())
                {
                    var date = args[0].AsDate().ToDateTime();
                    return format != null ?
                        date.ToString(format, cultureInfo) :
                        date.ToString(cultureInfo);
                }

                if (args[0].IsNumber())
                {
                    var num = args[0].AsNumber();
                    return format != null ?
                        num.ToString(format, cultureInfo) :
                        num.ToString(cultureInfo);
                }

                if (args[0].IsString())
                {
                    var s = args[0].AsString();
                    fixed (char* pValue = s)
                    {
                        var result = LazyStringParser.TryParseDateTime(pValue, s.Length, out DateTime dt, out _);
                        switch (result)
                        {
                            case LazyStringParser.Result.DateTime:
                                return format != null ?
                                    dt.ToString(format, cultureInfo) :
                                    dt.ToString(cultureInfo);
                            default:
                                throw new InvalidOperationException("toStringWithFormat(dateString) : 'dateString' is not a valid DateTime string");
                        }
                    }
                }

                if (args[0].IsBoolean() == false)
                {
                    throw new InvalidOperationException($"toStringWithFormat() is not supported for objects of type {args[0].Type} ");
                }

                var boolean = args[0].AsBoolean();
                return boolean.ToString(cultureInfo);
            }

            private static JsValue StartsWith(JsValue self, JsValue[] args)
            {
                if (args.Length != 2 || args[0].IsString() == false || args[1].IsString() == false)
                    throw new InvalidOperationException("startsWith(text, contained) must be called with two string parameters");

                return args[0].AsString().StartsWith(args[1].AsString(), StringComparison.OrdinalIgnoreCase);
            }

            private static JsValue EndsWith(JsValue self, JsValue[] args)
            {
                if (args.Length != 2 || args[0].IsString() == false || args[1].IsString() == false)
                    throw new InvalidOperationException("endsWith(text, contained) must be called with two string parameters");

                return args[0].AsString().EndsWith(args[1].AsString(), StringComparison.OrdinalIgnoreCase);
            }

            private JsValue Regex(JsValue self, JsValue[] args)
            {
                if (args.Length != 2 || args[0].IsString() == false || args[1].IsString() == false)
                    throw new InvalidOperationException("regex(text, regex) must be called with two string parameters");

                var regex = _regexCache.Get(args[1].AsString());

                return regex.IsMatch(args[0].AsString());
            }

            private static JsValue ScalarToRawString(JsValue self2, JsValue[] args)
            {
                if (args.Length != 2)
                    throw new InvalidOperationException("scalarToRawString(document, lambdaToField) may be called on with two parameters only");

                JsValue firstParam = args[0];
                if (firstParam.IsObject() && args[0].AsObject() is BlittableObjectInstance selfInstance)
                {
                    JsValue secondParam = args[1];
                    if (secondParam.IsObject() && secondParam.AsObject() is ArrowFunctionInstance lambda)
                    {
                        var functionAst = lambda.FunctionDeclaration;
                        var propName = functionAst.TryGetFieldFromSimpleLambdaExpression();

                        BlittableObjectInstance.BlittableObjectProperty existingValue = default;
                        if (selfInstance.OwnValues?.TryGetValue(propName, out existingValue) == true &&
                            existingValue != null)
                        {
                            if (existingValue.Changed)
                            {
                                return existingValue.Value;
                            }
                        }

                        var propertyIndex = selfInstance.Blittable.GetPropertyIndex(propName);

                        if (propertyIndex == -1)
                        {
                            return new ObjectInstance(selfInstance.Engine);
                        }

                        BlittableJsonReaderObject.PropertyDetails propDetails = new BlittableJsonReaderObject.PropertyDetails();
                        selfInstance.Blittable.GetPropertyByIndex(propertyIndex, ref propDetails);
                        var value = propDetails.Value;

                        switch (propDetails.Token & BlittableJsonReaderBase.TypesMask)
                        {
                            case BlittableJsonToken.Null:
                                return JsValue.Null;
                            case BlittableJsonToken.Boolean:
                                return (bool)propDetails.Value;
                            case BlittableJsonToken.Integer:
                                return new ObjectWrapper(selfInstance.Engine, value);
                            case BlittableJsonToken.LazyNumber:
                                return new ObjectWrapper(selfInstance.Engine, value);
                            case BlittableJsonToken.String:
                                return new ObjectWrapper(selfInstance.Engine, value);
                            case BlittableJsonToken.CompressedString:
                                return new ObjectWrapper(selfInstance.Engine, value);
                            default:
                                throw new InvalidOperationException("scalarToRawString(document, lambdaToField) lambda to field must return either raw numeric or raw string types");
                        }
                    }
                    else
                    {
                        throw new InvalidOperationException("scalarToRawString(document, lambdaToField) must be called with a second lambda argument");
                    }
                }
                else
                {
                    throw new InvalidOperationException("scalarToRawString(document, lambdaToField) may be called with a document first parameter only");
                }
            }

            private JsValue CmpXchangeInternal(string key)
            {
                if (string.IsNullOrEmpty(key))
                    return JsValue.Undefined;

                using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var value = _database.ServerStore.Cluster.GetCompareExchangeValue(ctx, key).Value;
                    if (value == null)
                        return JsValue.Null;

                    var jsValue = JavaScriptUtils.TranslateToJs(ScriptEngine, _jsonCtx, value.Clone(_jsonCtx));
                    return jsValue.AsObject().Get(Constants.CompareExchange.ObjectFieldName);
                }
            }

            private JsValue LoadDocumentInternal(string id)
            {
                if (string.IsNullOrEmpty(id))
                    return JsValue.Undefined;

                var document = _database.DocumentsStorage.Get(_docsCtx, id);

                if (DebugMode)
                {
                    DebugActions.LoadDocument.Add(new DynamicJsonValue
                    {
                        ["Id"] = id,
                        ["Exists"] = document != null
                    });
                }

                return JavaScriptUtils.TranslateToJs(ScriptEngine, _jsonCtx, document);
            }

            private JsValue[] _args = Array.Empty<JsValue>();
            private readonly JintPreventResolvingTasksReferenceResolver _refResolver = new JintPreventResolvingTasksReferenceResolver();

            public ScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, object[] args, QueryTimingsScope scope = null)
            {
                return Run(jsonCtx, docCtx, method, null, args, scope);
            }

            public ScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, string documentId, object[] args, QueryTimingsScope scope = null)
            {
                _docsCtx = docCtx;
                _jsonCtx = jsonCtx ?? ThrowArgumentNull();
                _scope = scope;

                JavaScriptUtils.Reset(_jsonCtx);

                Reset();
                OriginalDocumentId = documentId;

                SetArgs(jsonCtx, method, args);

                try
                {
                    var call = ScriptEngine.GetValue(method).TryCast<ICallable>();
                    var result = call.Call(Undefined.Instance, _args);
                    return new ScriptRunnerResult(this, result);
                }
                catch (JavaScriptException e)
                {
                    //ScriptRunnerResult is in charge of disposing of the disposable but it is not created (the clones did)
                    JavaScriptUtils.Clear();
                    throw CreateFullError(e);
                }
                catch (Exception)
                {
                    JavaScriptUtils.Clear();
                    throw;
                }
                finally
                {
                    _refResolver.ExplodeArgsOn(null, null);
                    _scope = null;
                    _loadScope = null;
                    _docsCtx = null;
                    _jsonCtx = null;
                    Array.Clear(_args, 0, _args.Length);
                }
            }

            private void SetArgs(JsonOperationContext jsonCtx, string method, object[] args)
            {
                if (_args.Length != args.Length)
                    _args = new JsValue[args.Length];
                for (var i = 0; i < args.Length; i++)
                    _args[i] = JavaScriptUtils.TranslateToJs(ScriptEngine, jsonCtx, args[i]);

                if (method != QueryMetadata.SelectOutput &&
                    _args.Length == 2 &&
                    _args[1].IsObject() &&
                    _args[1].AsObject() is BlittableObjectInstance boi)
                {
                    _refResolver.ExplodeArgsOn(null, boi);
                }
            }

            private static JsonOperationContext ThrowArgumentNull()
            {
                throw new ArgumentNullException("jsonCtx");
            }

            private Client.Exceptions.Documents.Patching.JavaScriptException CreateFullError(JavaScriptException e)
            {
                string msg;
                if (e.Error.IsString())
                    msg = e.Error.AsString();
                else if (e.Error.IsObject())
                    msg = JsBlittableBridge.Translate(_jsonCtx, ScriptEngine, e.Error.AsObject()).ToString();
                else
                    msg = e.Error.ToString();

                msg = "At " + e.Column + ":" + e.LineNumber + " " + msg;
                var javaScriptException = new Client.Exceptions.Documents.Patching.JavaScriptException(msg, e);
                return javaScriptException;
            }

            private void Reset()
            {
                if (DebugMode)
                {
                    if (DebugOutput == null)
                        DebugOutput = new List<string>();
                    if (DebugActions == null)
                        DebugActions = new PatchDebugActions();
                }

                Includes?.Clear();
                CompareExchangeValueIncludes?.Clear();
                DocumentCountersToUpdate?.Clear();
                DocumentTimeSeriesToUpdate?.Clear();
                PutOrDeleteCalled = false;
                OriginalDocumentId = null;
                RefreshOriginalDocument = false;
                ScriptEngine.ResetCallStack();
                ScriptEngine.ResetConstraints();
            }

            public object Translate(JsonOperationContext context, object o)
            {
                return JavaScriptUtils.TranslateToJs(ScriptEngine, context, o);
            }

            public object CreateEmptyObject()
            {
                return ScriptEngine.Object.Construct(Array.Empty<JsValue>());
            }

            public object Translate(ScriptRunnerResult result, JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
            {
                return Translate(result.RawJsValue, context, modifier, usageMode);
            }

            internal object Translate(JsValue val, JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
            {
                if (val.IsString())
                    return val.AsString();
                if (val.IsBoolean())
                    return val.AsBoolean();
                if (val.IsObject())
                {
                    if (val.IsNull())
                        return null;
                    return JsBlittableBridge.Translate(context, ScriptEngine, val.AsObject(), modifier, usageMode);
                }
                if (val.IsNumber())
                    return val.AsNumber();
                if (val.IsNull() || val.IsUndefined())
                    return null;
                if (val.IsArray())
                    throw new InvalidOperationException("Returning arrays from scripts is not supported, only objects or primitives");
                throw new NotSupportedException("Unable to translate " + val.Type);
            }
        }

        public struct ReturnRun : IDisposable
        {
            private SingleRun _run;
            private Holder _holder;

            public ReturnRun(SingleRun run, Holder holder)
            {
                _run = run;
                _holder = holder;
            }

            public void Dispose()
            {
                if (_run == null)
                    return;

                _run.ReadOnly = false;

                _run.DebugMode = false;
                _run.DebugOutput?.Clear();
                _run.DebugActions?.Clear();

                _run.Includes?.Clear();
                _run.CompareExchangeValueIncludes?.Clear();

                _run.OriginalDocumentId = null;
                _run.RefreshOriginalDocument = false;

                _run.DocumentCountersToUpdate?.Clear();
                _run.DocumentTimeSeriesToUpdate?.Clear();

                _holder.Parent._cache.Enqueue(_holder);
                _run = null;
            }
        }

        public bool RunIdleOperations()
        {
            while (_cache.TryDequeue(out var holder))
            {
                var val = holder.Value;
                if (val != null)
                {
                    // move the cache to weak reference value
                    holder.WeakValue = new WeakReference<SingleRun>(val);
                    holder.Value = null;
                    _cache.Enqueue(holder);
                    continue;
                }

                var weak = holder.WeakValue;
                if (weak == null)
                    continue;// no value, can discard it

                // The first item is a weak ref that wasn't clear?
                // The CLR can free it later, and then we'll act
                if (weak.TryGetTarget(out _))
                {
                    _cache.Enqueue(holder);
                    return true;
                }

                // the weak ref has no value, can discard it
            }

            return false;
        }
    }
}
