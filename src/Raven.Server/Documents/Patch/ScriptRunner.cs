extern alias NGC;
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
using JetBrains.Annotations;
using Jint;
using Jint.Native;
using Jint.Native.Array;
using Jint.Native.Function;
using Jint.Native.Object;
using Jint.Runtime;
using Jint.Runtime.Interop;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.Extensions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Spatial4n.Distance;
using ExpressionType = System.Linq.Expressions.ExpressionType;
using JavaScriptException = Jint.Runtime.JavaScriptException;

namespace Raven.Server.Documents.Patch
{
    public partial class ScriptRunner
    {
        public class Holder
        {
            public Holder(long generation)
            {
                Generation = generation;
            }

            public readonly long Generation;
            public ScriptRunner Parent;
            public SingleRun Value;
            public WeakReference<SingleRun> WeakValue;
        }

        public IJavaScriptOptions JsOptions;
        
        protected readonly ConcurrentQueue<Holder> _cache = new ConcurrentQueue<Holder>();
 
        private readonly ScriptRunnerCache _parent;
        internal readonly bool _enableClr;
        protected readonly DateTime _creationTime;
        public readonly List<string> ScriptsSource = new List<string>();

        public int NumberOfCachedScripts => _cache.Count(x =>
            x.Value != null ||
            x.WeakValue?.TryGetTarget(out _) == true);

        internal readonly Dictionary<string, DeclaredFunction> TimeSeriesDeclaration = new Dictionary<string, DeclaredFunction>();

        public long Runs;
        protected DateTime _lastRun;

        public string ScriptType { get; internal set; }
        
        public ScriptRunner([NotNull] ScriptRunnerCache parent, bool enableClr)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            JsOptions = jsOptions ?? db?.JsOptions ?? _configuration.JavaScript;
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

        public ReturnRun GetRunner(out SingleRun run, bool executeScriptsSource = true)
        {
            _lastRun = DateTime.UtcNow;
            Interlocked.Increment(ref Runs);
            Holder holder = GetSingleRunHolder(executeScriptsSource);
            run = holder.Value;
            return new ReturnRun(run, holder);
        }

        public Holder GetSingleRunHolder(bool executeScriptsSource = true)
        {
            SingleRun run = null;
            
            if (_cache.TryDequeue(out var holder) == false)
            {
                holder = new Holder(_parent.Generation)
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
                    holder.Value = new SingleRun(_parent.Database, _parent.Configuration, this, ScriptsSource, executeScriptsSource);
                }
            }

            return holder;
        }

        public void ReturnRunner(Holder holder)
        {
            if (holder == null)
                return;

            if (holder.Generation != _parent.Generation)
                return;

            _cache.Enqueue(holder);
        }

        public static void TryCompileScript(string script)
        {
            var jsEngineType = JsOptions.EngineType;
            IJsEngineHandle tryScriptEngineHandle = jsEngineType switch
            {
                JavaScriptEngineType.Jint => new PatchJint.JintEngineEx(),
                JavaScriptEngineType.V8 => GetSingleRunHolder().Value.ScriptEngineHandle,
                _ => throw new NotSupportedException($"Not supported JS engine type '{JsOptions}'.")
            };
            tryScriptEngineHandle.TryCompileScript(script);
        }

        public static unsafe DateTime GetDateArg(JsHandle arg, string signature, string argName)
        {
            if (arg.IsDate)
                return arg.AsDate;

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

        private static DateTime GetTimeSeriesDateArg(JsHandle arg, string signature, string argName)
        {
            if (arg.IsDate)
                return arg.AsDate;

            if (arg.IsStringEx == false)
                throw new ArgumentException($"{signature} : {argName} must be of type 'DateInstance' or a DateTime string. {GetTypes(arg)}");

            return TimeSeriesRetriever.ParseDateTime(arg.AsString);
        }
        
        private static string GetTypes(JsHandle value) => $"JintType({value.ValueType}) .NETType({value.GetType().Name})";
 
        
        public partial class SingleRun : IJavaScriptContext
        {
            public IJsEngineHandle ScriptEngineHandle;
            public JavaScriptUtilsBase JsUtilsBase;
            
            protected readonly DocumentDatabase _database;
            protected readonly RavenConfiguration _configuration;
            protected readonly IJavaScriptOptions _jsOptions;
            protected JavaScriptEngineType _jsEngineType;

            protected readonly ScriptRunner _runnerBase;
            protected QueryTimingsScope _scope;
            protected QueryTimingsScope _loadScope;
            protected DocumentsOperationContext _docsCtx;
            protected JsonOperationContext _jsonCtx;
            public PatchDebugActions DebugActions;
            public bool DebugMode;
            public List<string> DebugOutput;
            public bool PutOrDeleteCalled;
            public HashSet<string> Includes;
            public HashSet<string> IncludeRevisionsChangeVectors;
            public DateTime? IncludeRevisionByDateTimeBefore;
            public HashSet<string> CompareExchangeValueIncludes;
            protected HashSet<string> _documentIds;
            private CancellationToken _token;

            public bool ReadOnly
            {
                get => JsUtilsBase.ReadOnly;
                set => JsUtilsBase.ReadOnly = value;
            }
            
            public string OriginalDocumentId;
            public bool RefreshOriginalDocument;
            protected readonly ConcurrentLruRegexCache _regexCache = new ConcurrentLruRegexCache(1024);
            public HashSet<string> DocumentCountersToUpdate;
            public HashSet<string> DocumentTimeSeriesToUpdate;

            private JsHandle AppendTimeSeries;
            private JsHandle IncrementTimeSeries;
            private JsHandle DeleteRangeTimeSeries;
            private JsHandle GetRangeTimeSeries;
            private JsHandle GetStatsTimeSeries;
            
            protected const string _timeSeriesSignature = "timeseries(doc, name)";
            public const string GetMetadataMethod = "getMetadata";

            private List<string> _scriptsSource;

            public SingleRun(DocumentDatabase database, RavenConfiguration configuration, ScriptRunner runner, List<string> scriptsSource, bool executeScriptsSource = true)
            {
                _database = database;
                _configuration = configuration;
                _runnerBase = runner;
                _jsOptions = runner.JsOptions;
                _jsEngineType = _jsOptions.EngineType;

                _scriptsSource = scriptsSource; 
                InitializeEngineSpecific(executeScriptsSource);
            }
            
            ~SingleRun()
            {
                DisposeArgs();
                AppendTimeSeries.Dispose();
                IncrementTimeSeries.Dispose();
                DeleteRangeTimeSeries.Dispose();
                GetRangeTimeSeries.Dispose();
                GetStatsTimeSeries.Dispose();
                
                switch (_jsEngineType)
                {
                    case JavaScriptEngineType.Jint:
                        DisposeJint();
                        break;
                    case JavaScriptEngineType.V8:
                        DisposeV8();
                        break;
                    default:
                        throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
                }
            }

            public static InternalHandle DummyJsCallbackV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
            {
                throw new InvalidOperationException("Failed to set JS callback for V8");
            }
                
            public static JsValue DummyJsCallbackJint(JsValue self, JsValue[] args)
            {
                throw new InvalidOperationException("Failed to set JS callback for Jint");
            }

            public void SetContext()
            {
                switch (_jsEngineType)
                {
                    case JavaScriptEngineType.Jint:
                        SetContextJint();
                        break;
                    case JavaScriptEngineType.V8:
                        SetContextV8();
                        break;
                    default:
                        throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
                }
            }
                
            public void InitializeEngineSpecific(bool executeScriptsSource = true)
            {
                switch (_jsEngineType)
                {
                    case JavaScriptEngineType.Jint:
                        InitializeJint();
                        break;
                    case JavaScriptEngineType.V8:
                        InitializeV8();
                        break;
                    default:
                        throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
                }

                lock (ScriptEngineHandle)
                {
                    switch (_jsEngineType)
                    {
                        case JavaScriptEngineType.Jint:
                            InitializeLockedJint();
                            break;
                        case JavaScriptEngineType.V8:
                            InitializeLockedV8();
                            break;
                        default:
                            throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
                    }
                    
                    ScriptEngineHandle.SetGlobalClrCallBack("getMetadata",
                        (JsUtilsJint != null ? JsUtilsJint.GetMetadata : DummyJsCallbackJint, GetMetadataV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("metadataFor",
                        (JsUtilsJint != null ? JsUtilsJint.GetMetadata : DummyJsCallbackJint, GetMetadataV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("id",
                        (JsUtilsJint != null ? JsUtilsJint.GetDocumentId : DummyJsCallbackJint, GetDocumentIdV8));

                    ScriptEngineHandle.SetGlobalClrCallBack("output", (OutputDebugJint, OutputDebugV8));

                    //console.log
                    var consoleObject = ScriptEngineHandle.CreateObject();
                    var jsFuncLog = ScriptEngineHandle.CreateClrCallBack("log", (OutputDebugJint, OutputDebugV8), true);
                    consoleObject.FastAddProperty("log", jsFuncLog, false, false, false);
                    ScriptEngineHandle.SetGlobalProperty("console", consoleObject);

                    //spatial.distance
                    var spatialObject = ScriptEngineHandle.CreateObject();
                    var jsFuncSpatial = ScriptEngineHandle.CreateClrCallBack("distance", (Spatial_DistanceJint, Spatial_DistanceV8), true);
                    spatialObject.FastAddProperty("distance", jsFuncSpatial.Clone(), false, false, false);
                    ScriptEngineHandle.SetGlobalProperty("spatial", spatialObject);
                    ScriptEngineHandle.SetGlobalProperty("spatial.distance", jsFuncSpatial);

                    // includes
                    var includesObject = ScriptEngineHandle.CreateObject();
                    var jsFuncIncludeDocument = ScriptEngineHandle.CreateClrCallBack("include", (IncludeDocJint, IncludeDocV8), true);
                    includesObject.FastAddProperty("document", jsFuncIncludeDocument.Clone(), false, false, false);
                    // includes - backward compatibility
                    ScriptEngineHandle.SetGlobalProperty("include", jsFuncIncludeDocument);

                    var jsFuncIncludeCompareExchangeValue =
                        ScriptEngineHandle.CreateClrCallBack("cmpxchg", (IncludeCompareExchangeValueJint, IncludeCompareExchangeValueV8), true);
                    includesObject.FastAddProperty("cmpxchg", jsFuncIncludeCompareExchangeValue, false, false, false);

                    var jsFuncIncludeRevisions = ScriptEngineHandle.CreateClrCallBack("revisions", (IncludeRevisionsJint, IncludeRevisionsV8), true);
                    includesObject.FastAddProperty("revisions", jsFuncIncludeRevisions, false, false, false);
                    ScriptEngineHandle.SetGlobalProperty("includes", includesObject);

                    ScriptEngineHandle.SetGlobalClrCallBack("load", (LoadDocumentJint, LoadDocumentV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("LoadDocument", (ThrowOnLoadDocumentJint, ThrowOnLoadDocumentV8));

                    ScriptEngineHandle.SetGlobalClrCallBack("loadPath", (LoadDocumentByPathJint, LoadDocumentByPathV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("del", (DeleteDocumentJint, DeleteDocumentV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("DeleteDocument", (ThrowOnDeleteDocumentJint, ThrowOnDeleteDocumentV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("put", (PutDocumentJint, PutDocumentV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("PutDocument", (ThrowOnPutDocumentJint, ThrowOnPutDocumentV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("cmpxchg", (CompareExchangeJint, CompareExchangeV8));

                    ScriptEngineHandle.SetGlobalClrCallBack("counter", (GetCounterJint, GetCounterV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("counterRaw", (GetCounterRawJint, GetCounterRawV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("incrementCounter", (IncrementCounterJint, IncrementCounterV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("deleteCounter", (DeleteCounterJint, DeleteCounterV8));

                    ScriptEngineHandle.SetGlobalClrCallBack("lastModified", (GetLastModifiedJint, GetLastModifiedV8));

                    ScriptEngineHandle.SetGlobalClrCallBack("startsWith", (StartsWithJint, StartsWithV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("endsWith", (EndsWithJint, EndsWithV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("regex", (RegexJint, RegexV8));

                    ScriptEngineHandle.SetGlobalClrCallBack("Raven_ExplodeArgs", (ExplodeArgsJint, ExplodeArgsV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("Raven_Min", (Raven_MinJint, Raven_MinV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("Raven_Max", (Raven_MaxJint, Raven_MaxV8));

                    ScriptEngineHandle.SetGlobalClrCallBack("convertJsTimeToTimeSpanString", (ConvertJsTimeToTimeSpanStringJint, ConvertJsTimeToTimeSpanStringV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("convertToTimeSpanString", (ConvertToTimeSpanStringJint, ConvertToTimeSpanStringV8));
                    ScriptEngineHandle.SetGlobalClrCallBack("compareDates", (CompareDatesJint, CompareDatesV8));

                    ScriptEngineHandle.SetGlobalClrCallBack("toStringWithFormat", (ToStringWithFormatJint, ToStringWithFormatV8));

                    ScriptEngineHandle.SetGlobalClrCallBack("scalarToRawString", (ScalarToRawStringJint, ScalarToRawStringV8));

                    //TimeSeries
                    ScriptEngineHandle.SetGlobalClrCallBack("timeseries", (TimeSeriesJint, TimeSeriesV8));
                    ScriptEngineHandle.Execute(ScriptRunnerCache.PolyfillJs, "polyfill.js");

                    AppendTimeSeries = CreateJsHandle((null, AppendTimeSeriesV8));
                    IncrementTimeSeries = CreateJsHandle((null, IncrementTimeSeriesV8));
                    DeleteRangeTimeSeries = CreateJsHandle((null, DeleteRangeTimeSeriesV8));
                    GetRangeTimeSeries = CreateJsHandle((null, GetRangeTimeSeriesV8));
                    GetStatsTimeSeries = CreateJsHandle((null, GetStatsTimeSeriesV8));

                    if (executeScriptsSource)
                    {
                        ExecuteScriptsSource();
                    }

                    foreach (var ts in _runnerBase.TimeSeriesDeclaration)
                    {
                        ScriptEngineHandle.SetGlobalClrCallBack(ts.Key,
                            (
                                (self, args) => InvokeTimeSeriesFunctionJint(ts.Key, args),
                                (engine, isConstructCall, self, args) => InvokeTimeSeriesFunctionV8(ts.Key, args)
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

            public JsHandle CreateJsHandle((Func<JsValue, JsValue[], JsValue> Jint, JSFunction V8) funcTuple)
            {
                return _jsEngineType switch
                {
                    JavaScriptEngineType.Jint => JsHandle.Empty,
                    JavaScriptEngineType.V8 => new JsHandle(ScriptEngineV8.CreateClrCallBack(funcTuple.V8, true)),
                    _ => throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.")
                };

            }
            
            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public JsHandle TranslateToJs(JsonOperationContext context, object o, bool keepAlive = false)
            {
                return JsUtilsBase.TranslateToJs(context, o, keepAlive);
            }

            private JsHandle[] _args = Array.Empty<JsHandle>();

            private void SetArgs(JsonOperationContext jsonCtx, string method, object[] args)
            {
                if (_args.Length != args.Length)
                {
                    _args = new JsHandle[args.Length];
                }

                for (var i = 0; i < args.Length; i++)
                {
                    _args[i] = TranslateToJs(jsonCtx, args[i], false);
                }

                if (method != QueryMetadata.SelectOutput &&
                    _args.Length == 2 && _args[1].IsObject)
                {
                    switch (_jsEngineType)
                    {
                        case JavaScriptEngineType.Jint:
                            SetArgsJint();
                            break;
                        case JavaScriptEngineType.V8:
                            SetArgsV8();
                            break;
                        default:
                            throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
                    }
                }
            }

            private void DisposeArgs()
            {
                if (_args.Length == 0)
                        return;

                switch (_jsEngineType)
                {
                    case JavaScriptEngineType.Jint:
                        DisposeArgsJint();
                        break;
                    case JavaScriptEngineType.V8:
                        DisposeArgsV8();
                        break;
                    default:
                        throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.");
                }

                for (int i = 0; i < _args.Length; ++i)
                {
                    _args[i].Dispose();
                }
                Array.Clear(_args, 0, _args.Length);
            }
            
            private static readonly TimeSeriesStorage.AppendOptions AppendOptionsForScript = new TimeSeriesStorage.AppendOptions
            {
                AddNewNameToMetadata = false
            };

            public ScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, object[] args, QueryTimingsScope scope = null, CancellationToken token = default)
            {
                return Run(jsonCtx, docCtx, method, null, args, scope, token);
            }

            public ScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, string documentId, object[] args, QueryTimingsScope scope = null, CancellationToken token = default)
            {
                lock (ScriptEngineHandle)
                {
                    SetContext();
                    _lastException = null;
                    
                    _docsCtx = docCtx;
                    _jsonCtx = jsonCtx ?? ThrowArgumentNull();
                    _scope = scope;
                    _token = token;

                    JsUtilsBase.Reset(_jsonCtx);

                    Reset();
                    OriginalDocumentId = documentId;

                    bool isMemorySnapshotMade = false;
                            
                    try
                    {
                        if (ScriptEngineHandle.IsMemoryChecksOn)
                        {
                            ScriptEngineHandle.MakeSnapshot("single_run");
                        }

                        SetArgs(jsonCtx, method, args);

                        using (var jsMethod = ScriptEngineHandle.GetGlobalProperty(method))
                        {
                            if (jsMethod.IsUndefined)
                                throw new InvalidOperationException($"Failed to get global function '{method}', global object is: {ScriptEngineHandle.JsonStringify().StaticCall(ScriptEngineHandle.GlobalObject)}");
                            
                            if (!jsMethod.IsFunction)
                                throw new InvalidOperationException($"Obtained {method} global property is not a function: {ScriptEngineHandle.JsonStringify().StaticCall(method)}");

#if false //DEBUG
                            var argsStr = "";
                            for (int i = 0; i < _args.Length; i++)
                            {
                                using (var jsArgStr = ScriptEngineHandle.JsonStringify().StaticCall(_args[i]))
                                {
                                    var argStr = jsArgStr.IsUndefined ? "undefined" : jsArgStr.AsString;
                                    argsStr += argStr + "\n\n";
                                }
                            }
#endif
                            
                            using (var jsRes = jsMethod.StaticCall(_args))
                            {
                                if (jsRes.IsError)
                                {
                                    if (_lastException != null)
                                    {
                                        ExceptionDispatchInfo.Capture(_lastException).Throw();
                                    }
                                    else
                                    {
                                        jsRes.ThrowOnError();
                                    }
                                }
#if false //DEBUG
                                var resStr = "";
                                using (var jsResStr = ScriptEngineHandle.JsonStringify().StaticCall(jsRes))
                                {
                                    resStr = jsResStr.IsUndefined ? "undefined" : jsResStr.AsString;
                                }
#endif

                                if (ScriptEngineHandle.EngineType == JavaScriptEngineType.V8)
                                {
                                    ScriptEngineV8.AddToLastMemorySnapshotBefore(jsRes.V8.Item);
                                    isMemorySnapshotMade = true;
                                }

                                return new ScriptRunnerResult(this, jsRes);
                            }
                        }
                    }
                    catch (JavaScriptException e)
                    {
                        //ScriptRunnerResult is in charge of disposing of the disposable but it is not created (the clones did)
                        JsUtilsJint.Clear();
                        throw CreateFullError(e);
                    }
                    catch (V8Exception e)
                    {
                        //ScriptRunnerResult is in charge of disposing of the disposable but it is not created (the clones did)
                        JsUtilsV8.Clear();
                        throw CreateFullError(e);
                    }
                    catch (Exception)
                    {
                        JsUtilsBase.Clear();
                        throw;
                    }
                    finally
                    {
                        DisposeArgs();
                        _scope = null;
                        _loadScope = null;
                        _docsCtx = null;
                        _jsonCtx = null;
                        _token = default;
                        _lastException = null;

                        ScriptEngineHandle.ForceGarbageCollection();
                        if (ScriptEngineHandle.IsMemoryChecksOn && isMemorySnapshotMade)
                        {
                            ScriptEngineHandle.CheckForMemoryLeaks("single_run");
                        }
                    }
                }
            }


            private JsHandle InvokeTimeSeriesFunction(string name, params JsHandle[] args)
            {
                AssertValidDatabaseContext("InvokeTimeSeriesFunction");

                if (_runnerBase.TimeSeriesDeclaration.TryGetValue(name, out var func) == false)
                    throw new InvalidOperationException($"Failed to invoke time series function. Unknown time series name '{name}'.");

                object[] tsFunctionArgs = GetTimeSeriesFunctionArgs(name, args, out string docId, out var lazyIds);

                var queryParams = ((Document)tsFunctionArgs[^1]).Data;

                var retriever = new TimeSeriesRetriever(_docsCtx, queryParams, loadedDocuments: null, token: _token);

                var streamableResults = retriever.InvokeTimeSeriesFunction(func, docId, tsFunctionArgs, out var type);
                var result = retriever.MaterializeResults(streamableResults, type, addProjectionToResult: false, fromStudio: false);

                foreach (var id in lazyIds)
                {
                    id?.Dispose();
                }

                return TranslateToJs(_jsonCtx, result, true);
            }

            private object[] GetTimeSeriesFunctionArgs(string name, JsHandle[] args, out string docId, out List<IDisposable> lazyIds)
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
                return _jsEngineType switch
                {
                    JavaScriptEngineType.Jint => TranslateJint(context, o),
                    JavaScriptEngineType.V8 => TranslateV8(context, o),
                    _ => throw new NotSupportedException($"Not supported JS engine type '{_jsEngineType}'.")
                };
            }

            public JsHandle CreateEmptyObject()
            {
                return ScriptEngineHandle.CreateObject();
            }
            
            public object Translate(ScriptRunnerResult result, JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
            {
                return Translate(result.RawJsValue, context, modifier, usageMode);
            }

            internal object Translate(JsHandle jsValue, JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None, bool isRoot = true)
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
                    return _jsEngineType switch
                    {
                        JavaScriptEngineType.Jint => PatchJint.JsBlittableBridgeJint.Translate(context, ScriptEngineJint, jsValue.Jint.Obj, modifier, usageMode, isRoot: isRoot),
                        JavaScriptEngineType.V8 => PatchV8.JsBlittableBridgeV8.Translate(context, ScriptEngineV8, jsValue.V8.Item, modifier, usageMode, isRoot: isRoot),
                        _ => throw new NotSupportedException($"Not supported JS engine kind '{_jsEngineType}'.")
                    };
                }
                if (jsValue.IsNumberOrIntEx)
                    return jsValue.AsDouble;
                if (jsValue.IsNull || jsValue.IsUndefined)
                    return null;
                throw new NotSupportedException("Unable to translate " + jsValue.ValueType);
            }
            
            public override string ToString()
            {
                return string.Join(Environment.NewLine, _runnerBase.ScriptsSource);
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
                _run.IncludeRevisionsChangeVectors?.Clear();
                _run.IncludeRevisionByDateTimeBefore = null;

                _run.Includes?.Clear();
                _run.CompareExchangeValueIncludes?.Clear();

                _run.OriginalDocumentId = null;
                _run.RefreshOriginalDocument = false;

                _run.DocumentCountersToUpdate?.Clear();
                _run.DocumentTimeSeriesToUpdate?.Clear();

                _holder.Parent.ReturnRunner(_holder);
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
                    ReturnRunner(holder);
                    continue;
                }

                var weak = holder.WeakValue;
                if (weak == null)
                    continue;// no value, can discard it

                // The first item is a weak ref that wasn't clear?
                // The CLR can free it later, and then we'll act
                if (weak.TryGetTarget(out _))
                {
                    ReturnRunner(holder);
                    return true;
                }

                // the weak ref has no value, can discard it
            }

            return false;
        }
    }
}
