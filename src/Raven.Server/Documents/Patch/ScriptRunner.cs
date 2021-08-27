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
using V8.Net;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Session;
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
using Spatial4n.Core.Distance;
using ExpressionType = System.Linq.Expressions.ExpressionType;

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
                var engine = new V8Engine();
                using (var jsComiledScript = engine.Compile(script, "script", true))
                {}
            }
            catch (Exception e)
            {
                throw new JavaScriptParseException("Failed to parse:" + Environment.NewLine + script, e);
            }
        }

        public static unsafe DateTime GetDateArg(InternalHandle arg, string signature, string argName)
        {
            if (arg.IsDate)
                return arg.AsDate;

            if (arg.IsStringEx() == false)
                ThrowInvalidDateArgument();

            var s = arg.AsString;
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

        private static DateTime GetTimeSeriesDateArg(InternalHandle arg, string signature, string argName)
        {
            if (arg.IsDate)
                return arg.AsDate;

            if (arg.IsStringEx() == false)
                throw new ArgumentException($"{signature} : {argName} must be of type 'DateInstance' or a DateTime string. {GetTypes(arg)}");

            return TimeSeriesRetriever.ParseDateTime(arg.AsString);
        }
        
        private static string GetTypes(InternalHandle value) => $"JintType({value.ValueType}) .NETType({value.GetType().Name})";
        
        public class SingleRun
        {
            private readonly DocumentDatabase _database;
            private readonly RavenConfiguration _configuration;

            private readonly ScriptRunner _runner;
            public readonly V8EngineEx ScriptEngine;
            public JavaScriptUtils JavaScriptUtils;

            private QueryTimingsScope _scope;
            private QueryTimingsScope _loadScope;
            private DocumentsOperationContext _docsCtx;
            private JsonOperationContext _jsonCtx;
            public PatchDebugActions DebugActions;
            public bool DebugMode;
            public List<string> DebugOutput;
            public bool PutOrDeleteCalled;
            public HashSet<string> Includes;
            public HashSet<string> IncludeRevisionsChangeVectors;
            public DateTime? IncludeRevisionByDateTimeBefore;
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
            private const string _timeSeriesSignature = "timeseries(doc, name)";
            public const string GetMetadataMethod = "getMetadata";

            public SingleRun(DocumentDatabase database, RavenConfiguration configuration, ScriptRunner runner, List<string> scriptsSource)
            {
                _database = database;
                _configuration = configuration;
                _runner = runner;
                ScriptEngine = new V8EngineEx();

                string[] optionsCmd = {$"use_strict={configuration.Patching.StrictMode}"}; // TODO construct from options
                ScriptEngine.SetFlagsFromCommandLine(optionsCmd);
                        //.MaxStatements(indexConfiguration.MaxStepsForScript)
                        //.LocalTimeZone(TimeZoneInfo.Utc);  // -> harmony_intl_locale_info, harmony_intl_more_timezone

                JavaScriptUtils = new JavaScriptUtils(_runner, ScriptEngine);

                ScriptEngine.SetGlobalCLRCallBack(GetMetadataMethod, JavaScriptUtils.GetMetadata);
                ScriptEngine.SetGlobalCLRCallBack("id", JavaScriptUtils.GetDocumentId);

                ScriptEngine.SetGlobalCLRCallBack("output", OutputDebug);

                //console.log
                using (var consoleObject = ScriptEngine.CreateObject())
                {
                    consoleObject.FastAddProperty("log", ScriptEngine.CreateCLRCallBack(OutputDebug, true)._, false, false, false);
                    ScriptEngine.GlobalObject.SetProperty("console", consoleObject);
                }

                //spatial.distance
                using (var spatialObject = ScriptEngine.CreateObject())
                {
                    var spatialFunc = ScriptEngine.CreateCLRCallBack(Spatial_Distance, true);
                    spatialObject.FastAddProperty("distance", spatialFunc._, false, false, false);
                    ScriptEngine.GlobalObject.SetProperty("spatial", spatialObject);
                    ScriptEngine.GlobalObject.SetProperty("spatial.distance", spatialFunc._);
                }

                // includes
                using (var includesObject = ScriptEngine.CreateObject())
                {
                    var includeDocumentFunc = ScriptEngine.CreateCLRCallBack(IncludeDoc, true);
                    includesObject.FastAddProperty("document", includeDocumentFunc._, false, false, false);
                    includesObject.FastAddProperty("cmpxchg", ScriptEngine.CreateCLRCallBack(IncludeCompareExchangeValue, true)._, false, false, false);
                    includesObject.FastAddProperty("revisions", ScriptEngine.CreateCLRCallBack(IncludeRevisions, true)._, false, false, false);
                    ScriptEngine.GlobalObject.SetProperty("includes", includesObject);

                    // includes - backward compatibility
                    ScriptEngine.GlobalObject.SetProperty("include", includeDocumentFunc);
                }

                ScriptEngine.SetGlobalCLRCallBack("load", LoadDocument);
                ScriptEngine.SetGlobalCLRCallBack("LoadDocument", ThrowOnLoadDocument);

                ScriptEngine.SetGlobalCLRCallBack("loadPath", LoadDocumentByPath);
                ScriptEngine.SetGlobalCLRCallBack("del", DeleteDocument);
                ScriptEngine.SetGlobalCLRCallBack("DeleteDocument", ThrowOnDeleteDocument);
                ScriptEngine.SetGlobalCLRCallBack("put", PutDocument);
                ScriptEngine.SetGlobalCLRCallBack("PutDocument", ThrowOnPutDocument);
                ScriptEngine.SetGlobalCLRCallBack("cmpxchg", CompareExchange);

                ScriptEngine.SetGlobalCLRCallBack("counter", GetCounter);
                ScriptEngine.SetGlobalCLRCallBack("counterRaw", GetCounterRaw);
                ScriptEngine.SetGlobalCLRCallBack("incrementCounter", IncrementCounter);
                ScriptEngine.SetGlobalCLRCallBack("deleteCounter", DeleteCounter);

                ScriptEngine.SetGlobalCLRCallBack("lastModified", GetLastModified);

                ScriptEngine.SetGlobalCLRCallBack("startsWith", StartsWith);
                ScriptEngine.SetGlobalCLRCallBack("endsWith", EndsWith);
                ScriptEngine.SetGlobalCLRCallBack("regex", Regex);

                //ScriptEngine.SetGlobalCLRCallBack("Raven_ExplodeArgs", ExplodeArgs);
                ScriptEngine.SetGlobalCLRCallBack("Raven_Min", Raven_Min);
                ScriptEngine.SetGlobalCLRCallBack("Raven_Max", Raven_Max);

                ScriptEngine.SetGlobalCLRCallBack("convertJsTimeToTimeSpanString", ConvertJsTimeToTimeSpanString);
                ScriptEngine.SetGlobalCLRCallBack("convertToTimeSpanString", ConvertToTimeSpanString);
                ScriptEngine.SetGlobalCLRCallBack("compareDates", CompareDates);

                ScriptEngine.SetGlobalCLRCallBack("toStringWithFormat", ToStringWithFormat);

                //ScriptEngine.SetGlobalCLRCallBack("scalarToRawString", ScalarToRawString);

                //TimeSeries
                ScriptEngine.SetGlobalCLRCallBack("timeseries", TimeSeries);
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
                    ScriptEngine.SetGlobalCLRCallBack(ts.Key, (engine, isConstructCall, self, args) => InvokeTimeSeriesFunction(ts.Key, args));
                }
            }

            ~SingleRun()
            {
                DisposeArgs();
            }

            private (string Id, BlittableJsonReaderObject Doc) GetIdAndDocFromArg(InternalHandle docArg, string signature)
            {
                if (docArg.BoundObject is BlittableObjectInstance doc)
                    return (doc.DocumentId, doc.Blittable);

                if (docArg.IsStringEx())
                {
                    var id = docArg.AsString;
                    var document = _database.DocumentsStorage.Get(_docsCtx, id);
                    if (document == null)
                        throw new DocumentDoesNotExistException(id, "Cannot operate on a missing document.");

                    return (id, document.Data);
                }

                throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself. {GetTypes(docArg)}");
            }

            private string GetIdFromArg(InternalHandle docArg, string signature)
            {
                if (docArg.BoundObject is BlittableObjectInstance doc)
                    return doc.DocumentId;

                if (docArg.IsStringEx())
                {
                    var id = docArg.AsString;
                    return id;
                }

                throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself. {GetTypes(docArg)}");
            }

            private static string GetStringArg(InternalHandle jsArg, string signature, string argName)
            {
                if (jsArg.IsStringEx() == false)
                    throw new ArgumentException($"{signature}: The '{argName}' argument should be a string, but got {GetTypes(jsArg)}");
                return jsArg.AsString;
            }

            private void FillDoubleArrayFromJsArray(double[] array, InternalHandle jsArray, string signature)
            {
                int arrayLength = jsArray.ArrayLength;
                for (int i = 0; i < arrayLength; ++i)
                {
                    using (var jsItem = jsArray.GetProperty(i))
                    {
                        if (jsItem.IsNumberOrIntEx() == false)
                            throw new ArgumentException($"{signature}: The values argument must be an array of numbers, but got {jsItem.ValueType} key({i}) value({jsItem})");
                        array[i] = jsItem.AsDouble;
                    }
                }
            }

            private InternalHandle TimeSeries(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    AssertValidDatabaseContext(_timeSeriesSignature);

                    if (args.Length != 2)
                        throw new ArgumentException($"{_timeSeriesSignature}: This method requires 2 arguments but was called with {args.Length}");

                    var obj = ScriptEngine.CreateObject();
                    obj.SetProperty("append", ScriptEngine.CreateCLRCallBack(AppendTimeSeries, true)._);
                    obj.SetProperty("delete", ScriptEngine.CreateCLRCallBack(DeleteRangeTimeSeries, true)._);
                    obj.SetProperty("get", ScriptEngine.CreateCLRCallBack(GetRangeTimeSeries, true)._);
                    obj.SetProperty("doc", args[0]);
                    obj.SetProperty("name", args[1]);
                    obj.SetProperty("getStats", ScriptEngine.CreateCLRCallBack(GetStatsTimeSeries, true)._);

                    return obj;
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle GetStatsTimeSeries(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
            {
                try {
                    using (var document = self.GetProperty("doc"))
                    using (var name = self.GetProperty("name"))
                    {
                        var (id, doc) = GetIdAndDocFromArg(document, _timeSeriesSignature);

                        string timeSeries = GetStringArg(name, _timeSeriesSignature, "name");
                        var stats = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_docsCtx, id, timeSeries);

                        var tsStats = ScriptEngine.CreateObject();
                        tsStats.SetProperty(nameof(stats.Start), engine.CreateValue(stats.Start));
                        tsStats.SetProperty(nameof(stats.End), engine.CreateValue(stats.End));
                        tsStats.SetProperty(nameof(stats.Count), engine.CreateValue(stats.Count));
                        return tsStats;
                    }
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle AppendTimeSeries(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
            {
                try {
                    using (var document = self.GetProperty("doc"))
                    using (var name = self.GetProperty("name"))
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
                                if (tagArgument != null && tagArgument.IsNull == false && tagArgument.IsUndefined == false)
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
                            var jsValues = args[1];
                            Memory<double> values;
                            if (jsValues.IsArray)
                            {
                                valuesBuffer = ArrayPool<double>.Shared.Rent((int)jsValues.ArrayLength);
                                FillDoubleArrayFromJsArray(valuesBuffer, jsValues, signature);
                                values = new Memory<double>(valuesBuffer, 0, (int)jsValues.ArrayLength);
                            }
                            else if (jsValues.IsNumberOrIntEx())
                            {
                                valuesBuffer = ArrayPool<double>.Shared.Rent(1);
                                valuesBuffer[0] = jsValues.AsDouble;
                                values = new Memory<double>(valuesBuffer, 0, 1);
                            }
                            else
                            {
                                throw new ArgumentException($"{signature}: The values should be an array but got {GetTypes(jsValues)}");
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
                    }
                    return InternalHandle.Empty;
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle DeleteRangeTimeSeries(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
            {
                try {
                    using (var document = self.GetProperty("doc"))
                    using (var name = self.GetProperty("name"))
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
                            return InternalHandle.Empty;

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
                    }
                    return InternalHandle.Empty;
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle GetRangeTimeSeries(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
            {
                try {
                    using (var document = self.GetProperty("doc"))
                    using (var name = self.GetProperty("name"))
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

                        var entries = ScriptEngine.CreateArray(Array.Empty<InternalHandle>());
                        var noEntries = true;
                        foreach (var singleResult in reader.AllValues())
                        {
                            Span<double> valuesSpan = singleResult.Values.Span;
                            var jsSpanItems = new InternalHandle[valuesSpan.Length];
                            for (int i = 0; i < valuesSpan.Length; i++)
                            {
                                jsSpanItems[i] = ScriptEngine.CreateValue(valuesSpan[i]);
                            }
                            using (var jsValues = ScriptEngine.CreateArray(Array.Empty<InternalHandle>()))
                            {
                                //jsValues.FastAddProperty("length", ScriptEngine.CreateValue(0), true, false, false);
                                
                                using (var jsResPush = jsValues.Call("push", InternalHandle.Empty, jsSpanItems)) // KeepAlive to each item has been done earlier (upper)
                                    jsResPush.ThrowOnError(); // TODO check if is needed here
                                
                                if (noEntries)
                                    noEntries = false;

                                using (var entry = ScriptEngine.CreateObject())
                                {
                                    entry.SetProperty(nameof(TimeSeriesEntry.Timestamp), engine.CreateValue(singleResult.Timestamp.GetDefaultRavenFormat(isUtc: true)));
                                    entry.SetProperty(nameof(TimeSeriesEntry.Tag), engine.CreateValue(singleResult.Tag?.ToString()));
                                    entry.SetProperty(nameof(TimeSeriesEntry.Values), jsValues);
                                    entry.SetProperty(nameof(TimeSeriesEntry.IsRollup), engine.CreateValue(singleResult.Type == SingleResultType.RolledUp));
                                    
                                    using (var jsResPush = jsValues.Call("push", InternalHandle.Empty, entry))
                                        jsResPush.ThrowOnError(); // TODO check if is needed here
                                }
                            }
                            V8EngineEx.Dispose(jsSpanItems);

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

                        if (DebugMode && noEntries)
                        {
                            DebugActions.GetTimeSeries.Add(new DynamicJsonValue
                            {
                                ["Name"] = timeSeries,
                                ["Exists"] = false
                            });
                        }
                        return entries;
                    }
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private void GenericSortTwoElementArray(InternalHandle[] args, [CallerMemberName] string caller = null)
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
                        var a = V8EngineEx.ToNumber(args[0]);
                        var b = V8EngineEx.ToNumber(args[1]);
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
                                // if the string value is a number that is smaller than
                                // the numeric value, because Math.min(true, "-2") works :-(
                                if (double.TryParse(args[0].AsString, out double d) == false ||
                                    d > V8EngineEx.ToNumber(args[1]))
                                {
                                    Swap();
                                }
                                break;
                            case JSValueType.String:
                                if (string.Compare(args[0].AsString, args[1].AsString) > 0)
                                    Swap();
                                break;
                        }
                        break;
                    case JSValueType.Object:
                        throw new ArgumentException(caller + " cannot be called on an object");
                }
            }

            private InternalHandle Raven_Max(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    GenericSortTwoElementArray(args);
                    return args[1];
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle Raven_Min(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    GenericSortTwoElementArray(args);
                    return args[0];
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle IncludeDoc(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    if (args.Length != 1)
                        throw new InvalidOperationException("include(id) must be called with a single argument");

                    if (args[0].IsNull || args[0].IsUndefined)
                        return args[0];

                    if (args[0].IsArray)// recursive call ourselves
                    {
                        var jsArray = args[0];
                        int arrayLength = jsArray.ArrayLength;
                        for (int i = 0; i < arrayLength; ++i)
                        {
                            using (var jsItem = jsArray.GetProperty(i))
                            {
                                args[0].Set(jsItem);
                                if (args[0].IsStringEx())
                                    IncludeDoc(engine, isConstructCall, self, args);
                            }
                        }
                        return self;
                    }

                    if (args[0].IsStringEx() == false)
                        throw new InvalidOperationException("include(doc) must be called with an string or string array argument");

                    var id = args[0].AsString;

                    if (Includes == null)
                        Includes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                    Includes.Add(id);

                    InternalHandle jsRes = InternalHandle.Empty;
                    return jsRes.Set(self);
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle IncludeCompareExchangeValue(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    if (args.Length != 1)
                        throw new InvalidOperationException("includes.cmpxchg(key) must be called with a single argument");

                    InternalHandle jsRes = InternalHandle.Empty;

                    if (args[0].IsNull || args[0].IsUndefined)
                        return jsRes.Set(self);

                    if (args[0].IsArray)// recursive call ourselves
                    {
                        var jsArray = args[0];
                        int arrayLength = jsArray.ArrayLength;
                        for (int i = 0; i < arrayLength; ++i)
                        {
                            using (args[0] = jsArray.GetProperty(i))
                            {
                                if (args[0].IsStringEx())
                                    IncludeCompareExchangeValue(engine, isConstructCall, self, args);
                            }
                        }
                        args[0] = jsArray;
                        return jsRes.Set(self);
                    }

                    if (args[0].IsStringEx() == false)
                        throw new InvalidOperationException("includes.cmpxchg(key) must be called with an string or string array argument");

                    var key = args[0].AsString;

                    if (CompareExchangeValueIncludes == null)
                        CompareExchangeValueIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                    CompareExchangeValueIncludes.Add(key);

                    return jsRes.Set(self);
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            public override string ToString()
            {
                return string.Join(Environment.NewLine, _runner.ScriptsSource);
            }

            private InternalHandle GetLastModified(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    if (args.Length != 1)
                        throw new InvalidOperationException("lastModified(doc) must be called with a single argument");

                    if (args[0].IsNull || args[0].IsUndefined)
                        return args[0];

                    if (args[0].IsObject == false)
                        throw new InvalidOperationException("lastModified(doc) must be called with an object argument");

                    if (args[0].BoundObject is BlittableObjectInstance doc)
                    {
                        if (doc.LastModified == null)
                            return InternalHandle.Empty;

                        // we use UTC because last modified is in UTC
                        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                        var time = doc.LastModified.Value.Subtract(epoch)
                            .TotalMilliseconds;
                        return ScriptEngine.CreateValue(time);
                    }
                    return InternalHandle.Empty;
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle Spatial_Distance(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    if (args.Length < 4 && args.Length > 5)
                        throw new ArgumentException("Called with expected number of arguments, expected: spatial.distance(lat1, lng1, lat2, lng2, kilometers | miles | cartesian)");

                    for (int i = 0; i < 4; i++)
                    {
                        if (args[i].IsNumberOrIntEx() == false)
                            return InternalHandle.Empty;
                    }

                    var lat1 = args[0].AsDouble;
                    var lng1 = args[1].AsDouble;
                    var lat2 = args[2].AsDouble;
                    var lng2 = args[3].AsDouble;

                    var units = SpatialUnits.Kilometers;
                    if (args.Length > 4 && args[4].IsStringEx())
                    {
                        if (string.Equals("cartesian", args[4].AsString, StringComparison.OrdinalIgnoreCase))
                            return engine.CreateValue(SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator.CartesianDistance(lat1, lng1, lat2, lng2));

                        if (Enum.TryParse(args[4].AsString, ignoreCase: true, out units) == false)
                            throw new ArgumentException("Unable to parse units " + args[5] + ", expected: 'kilometers' or 'miles'");
                    }

                    var result = SpatialDistanceFieldComparatorSource.SpatialDistanceFieldComparator.HaverstineDistanceInMiles(lat1, lng1, lat2, lng2);
                    if (units == SpatialUnits.Kilometers)
                        result *= DistanceUtils.MILES_TO_KM;

                    return engine.CreateValue(result);
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle OutputDebug(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    InternalHandle jsRes = InternalHandle.Empty;
                    if (DebugMode == false)
                        return jsRes.Set(self);

                    InternalHandle obj = args[0];

                    DebugOutput.Add(GetDebugValue(obj, false));
                    return jsRes.Set(self);
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private string GetDebugValue(InternalHandle obj, bool recursive)
            {
                if (obj.IsStringEx())
                {
                    var debugValue = obj.ToString();
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
                            sb.Append(GetDebugValue(jsValue, true));
                    }
                    sb.Append("]");
                    return sb.ToString();
                }
                if (obj.IsObject)
                {
                    var result = new ScriptRunnerResult(this, obj);
                    using (var jsonObj = result.TranslateToObject(_jsonCtx))
                        return jsonObj.ToString();
                }
                if (obj.IsBoolean)
                    return obj.AsBoolean.ToString();
                if (obj.IsInt32)
                    return obj.AsInt32.ToString(CultureInfo.InvariantCulture);
                if (obj.IsNumberEx())
                    return obj.AsDouble.ToString(CultureInfo.InvariantCulture);
                if (obj.IsNull)
                    return "null";
                if (obj.IsUndefined)
                    return "undefined";
                return obj.ToString();
            }

            public InternalHandle PutDocument(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    string changeVector = null;

                    if (args.Length != 2 && args.Length != 3)
                        throw new InvalidOperationException("put(id, doc, changeVector) must be called with called with 2 or 3 arguments only");
                    AssertValidDatabaseContext("put document");
                    AssertNotReadOnly();
                    if (args[0].IsStringEx() == false && args[0].IsNull == false && args[0].IsUndefined == false)
                        AssertValidId();

                    var id = args[0].IsNull || args[0].IsUndefined ? null : args[0].AsString;

                    if (args[1].IsObject == false)
                        throw new InvalidOperationException(
                            $"Created document must be a valid object which is not null or empty. Document ID: '{id}'.");

                    PutOrDeleteCalled = true;

                    if (args.Length == 3)
                        if (args[2].IsStringEx())
                            changeVector = args[2].AsString;
                        else if (args[2].IsNull == false && args[0].IsUndefined == false)
                            throw new InvalidOperationException(
                                $"The change vector must be a string or null. Document ID: '{id}'.");

                    BlittableJsonReaderObject reader = null;
                    try
                    {
                        reader = JsBlittableBridge.Translate(_jsonCtx, ScriptEngine, args[1].Object, usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);

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

                        return engine.CreateValue(put.Id);
                    }
                    finally
                    {
                        if (DebugMode == false)
                            reader?.Dispose();
                    }
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private static void AssertValidId()
            {
                throw new InvalidOperationException("The first parameter to put(id, doc, changeVector) must be a string");
            }

            public InternalHandle DeleteDocument(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    if (args.Length != 1 && args.Length != 2)
                        throw new InvalidOperationException("delete(id, changeVector) must be called with at least one parameter");

                    if (args[0].IsStringEx() == false)
                        throw new InvalidOperationException("delete(id, changeVector) id argument must be a string");

                    var id = args[0].AsString;
                    string changeVector = null;

                    if (args.Length == 2 && args[1].IsStringEx())
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

                    return engine.CreateValue(result != null);
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
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

            private InternalHandle IncludeRevisions(V8Engine engine, bool isConstructCall, InternalHandle self, InternalHandle[] args)
            {
                if (args == null)
                    return ScriptEngine.CreateNullValue();

                IncludeRevisionsChangeVectors ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                
                foreach (InternalHandle arg in args)
                {
                    switch (arg.ValueType)
                    {
                        case JSValueType.String:
                            if (DateTime.TryParseExact(arg.ToString(), DefaultFormat.DateTimeFormatsToRead, CultureInfo.InvariantCulture,DateTimeStyles.AssumeUniversal, out var dateTime))
                            {
                                IncludeRevisionByDateTimeBefore = dateTime.ToUniversalTime();
                                continue;
                            }
                            IncludeRevisionsChangeVectors.Add(arg.ToString());
                            break;
                        case JSValueType.Object when arg.IsArray:
                            InternalHandle jsArray = arg;
                            int arrayLength = jsArray.ArrayLength;
                            for (int i = 0; i < arrayLength; ++i)
                            {
                                using (var jsItem = jsArray.GetProperty(i))
                                {
                                    if (jsItem.IsStringEx() == false)
                                        continue;
                                    IncludeRevisionsChangeVectors.Add(jsItem.ToString());
                                }
                            }
                            break;
                    }
                }
                
                return ScriptEngine.CreateNullValue();
            }
            
            private InternalHandle LoadDocumentByPath(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    using (_loadScope = _loadScope?.Start() ?? _scope?.For(nameof(QueryTimingsScope.Names.Load)))
                    {
                        AssertValidDatabaseContext("loadPath");

                        if (args.Length != 2 ||
                            (args[0].IsNull == false && args[0].IsUndefined == false && args[0].IsObject == false)
                            || args[1].IsStringEx() == false)
                            throw new InvalidOperationException("loadPath(doc, path) must be called with a document and path");

                        if (args[0].IsNull || args[1].IsUndefined)
                            return args[0];

                        if (args[0].BoundObject is BlittableObjectInstance b)
                        {
                            var path = args[1].AsString;
                            if (_documentIds == null)
                                _documentIds = new HashSet<string>();

                            _documentIds.Clear();
                            IncludeUtil.GetDocIdFromInclude(b.Blittable, path, _documentIds, _database.IdentityPartsSeparator);
                            if (path.IndexOf("[]", StringComparison.InvariantCulture) != -1) // array
                                return ScriptEngine.FromObject(_documentIds.Select(LoadDocumentInternal).ToList());
                            if (_documentIds.Count == 0)
                                return ScriptEngine.CreateNullValue();

                            return LoadDocumentInternal(_documentIds.First());
                        }

                        throw new InvalidOperationException("loadPath(doc, path) must be called with a valid document instance, but got a JS object instead");
                    }
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle CompareExchange(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    AssertValidDatabaseContext("cmpxchg");

                    if (args.Length != 1 && args.Length != 2 || args[0].IsStringEx() == false)
                        throw new InvalidOperationException("cmpxchg(key) must be called with a single string argument");

                    return CmpXchangeInternal(CompareExchangeKey.GetStorageKey(_database.Name, args[0].AsString));
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle LoadDocument(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    using (_loadScope = _loadScope?.Start() ?? _scope?.For(nameof(QueryTimingsScope.Names.Load)))
                    {
                        AssertValidDatabaseContext("load");

                        if (args.Length != 1)
                            throw new InvalidOperationException($"load(id | ids) must be called with a single string argument");

                        if (args[0].IsNull || args[0].IsUndefined)
                            return args[0];

                        if (args[0].IsArray)
                        {
                            var results = ScriptEngine.CreateArray(Array.Empty<InternalHandle>());
                            var jsArray = args[0];
                            int arrayLength = jsArray.ArrayLength;
                            for (int i = 0; i < arrayLength; ++i)
                            {
                                using (var jsItem = jsArray.GetProperty(i))
                                {
                                    if (jsItem.IsStringEx() == false)
                                        throw new InvalidOperationException("load(ids) must be called with a array of strings, but got " + jsItem.ValueType + " - " + jsItem.ToString());
                                    using (var result = LoadDocumentInternal(jsItem.AsString))
                                    using (var jsResPush = results.Call("push", InternalHandle.Empty, result))
                                        jsResPush.ThrowOnError(); // TODO check if is needed here
                                }
                            }
                            return results;
                        }

                        if (args[0].IsStringEx() == false)
                            throw new InvalidOperationException("load(id | ids) must be called with a single string or array argument");

                        return LoadDocumentInternal(args[0].AsString);
                    }
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle GetCounter(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    return GetCounterInternal(args);
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle GetCounterRaw(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    return GetCounterInternal(args, true);
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle GetCounterInternal(InternalHandle[] args, bool raw = false)
            {
                var signature = raw ? "counterRaw(doc, name)" : "counter(doc, name)";
                AssertValidDatabaseContext(signature);

                if (args.Length != 2)
                    throw new InvalidOperationException($"{signature} must be called with exactly 2 arguments");

                string id;
                if (args[0].BoundObject is BlittableObjectInstance doc)
                {
                    id = doc.DocumentId;
                }
                else if (args[0].IsStringEx())
                {
                    id = args[0].AsString;
                }
                else
                {
                    throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself");
                }

                if (args[1].IsStringEx() == false)
                {
                    throw new InvalidOperationException($"{signature}: 'name' must be a string argument");
                }

                var name = args[1].AsString;
                if (string.IsNullOrEmpty(id) || string.IsNullOrEmpty(name))
                {
                    return InternalHandle.Empty;
                }

                if (raw == false)
                {
                    var counterValue = ScriptEngine.CreateValue(_database.DocumentsStorage.CountersStorage.GetCounterValue(_docsCtx, id, name)?.Value ?? null);

                    if (DebugMode)
                    {
                        DebugActions.GetCounter.Add(new DynamicJsonValue
                        {
                            ["Name"] = name,
                            ["Value"] = counterValue.ToString(),
                            ["Exists"] = counterValue.IsNull == false
                        });
                    }

                    return counterValue;
                }

                var rawValues = ScriptEngine.CreateObject();
                foreach (var partialValue in _database.DocumentsStorage.CountersStorage.GetCounterPartialValues(_docsCtx, id, name))
                {
                    using (var jsPartialValue = ScriptEngine.CreateValue(partialValue.PartialValue))
                        rawValues.FastAddProperty(partialValue.ChangeVector, jsPartialValue, true, false, false);
                }

                return rawValues;
            }

            private InternalHandle IncrementCounter(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    AssertValidDatabaseContext("incrementCounter");

                    if (args.Length < 2 || args.Length > 3)
                    {
                        ThrowInvalidIncrementCounterArgs(args);
                    }

                    var signature = args.Length == 2 ? "incrementCounter(doc, name)" : "incrementCounter(doc, name, value)";

                    BlittableJsonReaderObject docBlittable = null;
                    string id = null;

                    if (args[0].BoundObject is BlittableObjectInstance doc)
                    {
                        id = doc.DocumentId;
                        docBlittable = doc.Blittable;
                    }
                    else if (args[0].IsStringEx())
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

                    if (args[1].IsStringEx() == false)
                        ThrowInvalidCounterName(signature);

                    var name = args[1].AsString;
                    if (string.IsNullOrWhiteSpace(name))
                        ThrowInvalidCounterName(signature);

                    double value = 1;
                    if (args.Length == 3)
                    {
                        if (args[2].IsNumberOrIntEx() == false)
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

                    return engine.CreateValue(true);
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle DeleteCounter(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    AssertValidDatabaseContext("deleteCounter");

                    if (args.Length != 2)
                    {
                        ThrowInvalidDeleteCounterArgs();
                    }

                    string id = null;
                    BlittableJsonReaderObject docBlittable = null;

                    if (args[0].BoundObject is BlittableObjectInstance doc)
                    {
                        id = doc.DocumentId;
                        docBlittable = doc.Blittable;
                    }
                    else if (args[0].IsStringEx())
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

                    if (args[1].IsStringEx() == false)
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

                    return engine.CreateValue(true);
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle InvokeTimeSeriesFunction(string name, params InternalHandle[] args)
            {
                AssertValidDatabaseContext("InvokeTimeSeriesFunction");

                if (_runner.TimeSeriesDeclaration.TryGetValue(name, out var func) == false)
                    throw new InvalidOperationException($"Failed to invoke time series function. Unknown time series name '{name}'.");

                object[] tsFunctionArgs = GetTimeSeriesFunctionArgs(name, args, out string docId, out var lazyIds);

                var queryParams = ((Document)tsFunctionArgs[^1]).Data;

                var retriever = new TimeSeriesRetriever(_docsCtx, queryParams, null);

                var streamableResults = retriever.InvokeTimeSeriesFunction(func, docId, tsFunctionArgs, out var type);
                var result = retriever.MaterializeResults(streamableResults, type, addProjectionToResult: false, fromStudio: false);

                foreach (var id in lazyIds)
                {
                    id?.Dispose();
                }

                return JavaScriptUtils.TranslateToJs(_jsonCtx, result);
            }

            private object[] GetTimeSeriesFunctionArgs(string name, InternalHandle[] args, out string docId, out List<IDisposable> lazyIds)
            {
                var tsFunctionArgs = new object[args.Length + 1];
                docId = null;

                lazyIds = new List<IDisposable>();

                for (var index = 0; index < args.Length; index++)
                {
                    if (args[index].BoundObject is BlittableObjectInstance boi)
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
                        !(_args[0].BoundObject is BlittableObjectInstance originalDoc))
                        throw new InvalidOperationException($"Failed to invoke time series function '{name}'. Couldn't find the document ID to operate on. " +
                                                            "A Document instance argument was not provided to the time series function or to the ScriptRunner");

                    docId = originalDoc.DocumentId;
                }

                if (_args[_args.Length - 1].IsObject == false || !(_args[_args.Length - 1].BoundObject is BlittableObjectInstance queryParams))
                    throw new InvalidOperationException($"Failed to invoke time series function '{name}'. ScriptRunner is missing QueryParameters argument");

                tsFunctionArgs[tsFunctionArgs.Length - 1] = new Document
                {
                    Data = queryParams.Blittable
                };

                return tsFunctionArgs;
            }

            private static void ThrowInvalidIncrementCounterArgs(InternalHandle[] args)
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

            private static InternalHandle ThrowOnLoadDocument(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    throw new MissingMethodException("The method LoadDocument was renamed to 'load'");
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private static InternalHandle ThrowOnPutDocument(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    throw new MissingMethodException("The method PutDocument was renamed to 'put'");
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private static InternalHandle ThrowOnDeleteDocument(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    throw new MissingMethodException("The method DeleteDocument was renamed to 'del'");
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle ConvertJsTimeToTimeSpanString(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    if (args.Length != 1 || args[0].IsNumberOrIntEx() == false)
                        throw new InvalidOperationException("convertJsTimeToTimeSpanString(ticks) must be called with a single long argument");

                    var ticks = Convert.ToInt64(args[0].AsDouble) * 10000;

                    var asTimeSpan = new TimeSpan(ticks);

                    return engine.CreateValue(asTimeSpan.ToString());
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle ConvertToTimeSpanString(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    if (args.Length == 1)
                    {
                        if (args[0].IsNumberOrIntEx() == false)
                            throw new InvalidOperationException("convertToTimeSpanString(ticks) must be called with a single long argument");

                        var ticks = Convert.ToInt64(args[0].AsDouble);
                        var asTimeSpan = new TimeSpan(ticks);
                        return engine.CreateValue(asTimeSpan.ToString());
                    }

                    if (args.Length == 3)
                    {
                        if (args[0].IsNumberOrIntEx() == false || args[1].IsNumberOrIntEx() == false || args[2].IsNumberOrIntEx() == false)
                            throw new InvalidOperationException("convertToTimeSpanString(hours, minutes, seconds) must be called with integer values");

                        var hours = Convert.ToInt32(args[0].AsDouble);
                        var minutes = Convert.ToInt32(args[1].AsDouble);
                        var seconds = Convert.ToInt32(args[2].AsDouble);

                        var asTimeSpan = new TimeSpan(hours, minutes, seconds);
                        return engine.CreateValue(asTimeSpan.ToString());
                    }

                    if (args.Length == 4)
                    {
                        if (args[0].IsNumberOrIntEx() == false || args[1].IsNumberOrIntEx() == false || args[2].IsNumberOrIntEx() == false || args[3].IsNumberOrIntEx() == false)
                            throw new InvalidOperationException("convertToTimeSpanString(days, hours, minutes, seconds) must be called with integer values");

                        var days = Convert.ToInt32(args[0].AsDouble);
                        var hours = Convert.ToInt32(args[1].AsDouble);
                        var minutes = Convert.ToInt32(args[2].AsDouble);
                        var seconds = Convert.ToInt32(args[3].AsDouble);

                        var asTimeSpan = new TimeSpan(days, hours, minutes, seconds);
                        return engine.CreateValue(asTimeSpan.ToString());
                    }

                    if (args.Length == 5)
                    {
                        if (args[0].IsNumberOrIntEx() == false || args[1].IsNumberOrIntEx() == false || args[2].IsNumberOrIntEx() == false || args[3].IsNumberOrIntEx() == false || args[4].IsNumberOrIntEx() == false)
                            throw new InvalidOperationException("convertToTimeSpanString(days, hours, minutes, seconds, milliseconds) must be called with integer values");

                        var days = Convert.ToInt32(args[0].AsDouble);
                        var hours = Convert.ToInt32(args[1].AsDouble);
                        var minutes = Convert.ToInt32(args[2].AsDouble);
                        var seconds = Convert.ToInt32(args[3].AsDouble);
                        var milliseconds = Convert.ToInt32(args[4].AsDouble);

                        var asTimeSpan = new TimeSpan(days, hours, minutes, seconds, milliseconds);
                        return engine.CreateValue(asTimeSpan.ToString());
                    }

                    throw new InvalidOperationException("supported overloads are: " +
                                                        "convertToTimeSpanString(ticks), " +
                                                        "convertToTimeSpanString(hours, minutes, seconds), " +
                                                        "convertToTimeSpanString(days, hours, minutes, seconds), " +
                                                        "convertToTimeSpanString(days, hours, minutes, seconds, milliseconds)");
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private static InternalHandle CompareDates(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
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
                    else if (args[2].IsStringEx() == false ||
                            Enum.TryParse(args[2].AsString, out binaryOperationType) == false)
                    {
                        throw new InvalidOperationException("compareDates(date1, date2, operationType) : 'operationType' must be a string argument representing a valid 'ExpressionType'");
                    }

                    dynamic date1, date2;
                    if ((binaryOperationType == ExpressionType.Equal ||
                        binaryOperationType == ExpressionType.NotEqual) &&
                        args[0].IsStringEx() && args[1].IsStringEx())
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
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private unsafe InternalHandle ToStringWithFormat(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    if (args.Length < 1 || args.Length > 3)
                    {
                        throw new InvalidOperationException($"No overload for method 'toStringWithFormat' takes {args.Length} arguments. " +
                                                            "Supported overloads are : toStringWithFormat(object), toStringWithFormat(object, format), toStringWithFormat(object, culture), toStringWithFormat(object, format, culture).");
                    }

                    var cultureInfo = CultureInfo.InvariantCulture;
                    string format = null;

                    for (var i = 1; i < args.Length; i++)
                    {
                        if (args[i].IsStringEx() == false)
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
                        return engine.CreateValue(format != null ?
                            date.ToString(format, cultureInfo) :
                            date.ToString(cultureInfo));
                    }

                    if (args[0].IsNumberOrIntEx())
                    {
                        var num = args[0].AsDouble;
                        return engine.CreateValue(format != null ?
                            num.ToString(format, cultureInfo) :
                            num.ToString(cultureInfo));
                    }

                    if (args[0].IsStringEx())
                    {
                        var s = args[0].AsString;
                        fixed (char* pValue = s)
                        {
                            var result = LazyStringParser.TryParseDateTime(pValue, s.Length, out DateTime dt, out _);
                            switch (result)
                            {
                                case LazyStringParser.Result.DateTime:
                                    return engine.CreateValue(format != null ?
                                        dt.ToString(format, cultureInfo) :
                                        dt.ToString(cultureInfo));
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
                    return engine.CreateValue(boolean.ToString(cultureInfo));
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle StartsWith(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    if (args.Length != 2 || args[0].IsStringEx() == false || args[1].IsStringEx() == false)
                        throw new InvalidOperationException("startsWith(text, contained) must be called with two string parameters");

                    return engine.CreateValue(args[0].AsString.StartsWith(args[1].AsString, StringComparison.OrdinalIgnoreCase));
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle EndsWith(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    if (args.Length != 2 || args[0].IsStringEx() == false || args[1].IsStringEx() == false)
                        throw new InvalidOperationException("endsWith(text, contained) must be called with two string parameters");

                    return engine.CreateValue(args[0].AsString.EndsWith(args[1].AsString, StringComparison.OrdinalIgnoreCase));
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            private InternalHandle Regex(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try {
                    if (args.Length != 2 || args[0].IsStringEx() == false || args[1].IsStringEx() == false)
                        throw new InvalidOperationException("regex(text, regex) must be called with two string parameters");

                    var regex = _regexCache.Get(args[1].AsString);

                    return engine.CreateValue(regex.IsMatch(args[0].AsString));
                }
                catch (Exception e) 
                {
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            /*private InternalHandle ScalarToRawString(InternalHandle self2, params InternalHandle[] args)
            {
                if (args.Length != 2)
                    throw new InvalidOperationException("scalarToRawString(document, lambdaToField) may be called on with two parameters only");

                var firstParam = args[0];
                if (firstParam.BoundObject != null && args[0].BoundObject is BlittableObjectInstance selfInstance)
                {
                    var secondParam = args[1];
                    if (secondParam.IsObject && secondParam.Object is V8Function lambda) // Jint: is ScriptFunctionInstance lambda)
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
                            return new selfInstance.ScriptEngine.CreateObject();
                        }

                        BlittableJsonReaderObject.PropertyDetails propDetails = new BlittableJsonReaderObject.PropertyDetails();
                        selfInstance.Blittable.GetPropertyByIndex(propertyIndex, ref propDetails);
                        var value = propDetails.Value;

                        switch (propDetails.Token & BlittableJsonReaderBase.TypesMask)
                        {
                            case BlittableJsonToken.Null:
                                return selfInstance.ScriptEngine.CreateNullValue();
                            case BlittableJsonToken.Boolean:
                                return selfInstance.ScriptEngine.CreateValue((bool)value);
                            case BlittableJsonToken.Integer:
                                return selfInstance.ScriptEngine.FromObject(value); // or ObjectBinder? instead of ObjectWrapper
                            case BlittableJsonToken.LazyNumber:
                                return selfInstance.ScriptEngine.FromObject(value);  // or ObjectBinder? instead of ObjectWrapper // potentially could be BlittableObjectInstance.BlittableObjectProperty.GetJsValueForLazyNumber(selfInstance.JavaScriptUtils, (LazyNumberValue)value);
                            case BlittableJsonToken.String:
                                return selfInstance.ScriptEngine.CreateValue(value.ToString());
                            case BlittableJsonToken.CompressedString:
                                return selfInstance.ScriptEngine.CreateValue(value.ToString());
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
            }*/

            private InternalHandle CmpXchangeInternal(string key)
            {
                if (string.IsNullOrEmpty(key))
                    return InternalHandle.Empty;

                using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var value = _database.ServerStore.Cluster.GetCompareExchangeValue(ctx, key).Value;
                    if (value == null)
                        return ScriptEngine.CreateNullValue();

                    using (var jsValue = JavaScriptUtils.TranslateToJs(_jsonCtx, value.Clone(_jsonCtx)))
                        return jsValue.GetProperty(Constants.CompareExchange.ObjectFieldName);
                }
            }

            private InternalHandle LoadDocumentInternal(string id)
            {
                if (string.IsNullOrEmpty(id))
                    return InternalHandle.Empty;

                var document = _database.DocumentsStorage.Get(_docsCtx, id);

                if (DebugMode)
                {
                    DebugActions.LoadDocument.Add(new DynamicJsonValue
                    {
                        ["Id"] = id,
                        ["Exists"] = document != null
                    });
                }

                return JavaScriptUtils.TranslateToJs(_jsonCtx, document);
            }

            private InternalHandle[] _args = Array.Empty<InternalHandle>();

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
                    using (var jsMethod = ScriptEngine.GlobalObject.GetProperty(method))
                    {
                        //if (jsMethod.IsFunction) {
                            using (var jsRes = jsMethod.StaticCall(_args))
                            {
                                jsRes.ThrowOnError(); // TODO check if is needed here
                                return new ScriptRunnerResult(this, jsRes);
                            }
                        //}
                    }
                }
                catch (V8Exception e)
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
                    _scope = null;
                    _loadScope = null;
                    _docsCtx = null;
                    _jsonCtx = null;
                    DisposeArgs();
                }
            }

            private void SetArgs(JsonOperationContext jsonCtx, string method, object[] args)
            {
                if (_args.Length != args.Length)
                {
                    DisposeArgs();
                    _args = new InternalHandle[args.Length];
                }
                for (var i = 0; i < args.Length; i++)
                    _args[i] = JavaScriptUtils.TranslateToJs(jsonCtx, args[i]);

                /*if (method != QueryMetadata.SelectOutput &&
                    _args.Length == 2 &&
                    _args[1].BoundObject is BlittableObjectInstance boi)
                {
                    _refResolver.ExplodeArgsOn(null, boi);
                }*/
            }

            private static JsonOperationContext ThrowArgumentNull()
            {
                throw new ArgumentNullException("jsonCtx");
            }

            private JavaScriptException CreateFullError(V8Exception e)
            {
                /*string msg;
                if (e.Handle.IsStringEx())
                    msg = e.Handle.AsString;
                else if (e.Handle.IsObject)
                    msg = JsBlittableBridge.Translate(_jsonCtx, ScriptEngine, e.Handle).ToString();
                else
                    msg = e.Handle.ToString();

                msg = "At " + e.Column + ":" + e.LineNumber + " " + msg;
                var javaScriptException = new JavaScriptException(msg, e);*/

                var javaScriptException = new JavaScriptException(e.Message, e);
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
                IncludeRevisionsChangeVectors?.Clear();
                IncludeRevisionByDateTimeBefore = null;
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
                return JavaScriptUtils.TranslateToJs(context, o);
            }

            public object CreateEmptyObject()
            {
                return ScriptEngine.CreateObject();
            }

            public object Translate(ScriptRunnerResult result, JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
            {
                return Translate(result.RawJsValue, context, modifier, usageMode);
            }

            internal object Translate(InternalHandle jsValue, JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
            {
                if (jsValue.IsStringEx())
                    return jsValue.AsString;
                if (jsValue.IsBoolean)
                    return jsValue.AsBoolean;
                if (jsValue.IsObject)
                {
                    if (jsValue.IsNull)
                        return null;
                    return JsBlittableBridge.Translate(context, ScriptEngine, jsValue, modifier, usageMode);
                }
                if (jsValue.IsNumberOrIntEx())
                    return jsValue.AsDouble;
                if (jsValue.IsNull || jsValue.IsUndefined)
                    return null;
                if (jsValue.IsArray)
                    throw new InvalidOperationException("Returning arrays from scripts is not supported, only objects or primitives");
                throw new NotSupportedException("Unable to translate " + jsValue.ValueType);
             }

            private void DisposeArgs()
            {
                for (int i = 0; i < _args.Length; ++i)
                {
                    _args[i].Dispose();
                    Array.Clear(_args, 0, _args.Length);
                }
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
