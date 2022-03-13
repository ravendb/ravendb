using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;
using System.Text;
using Jint.Native;
using V8.Net;
using Raven.Server.Extensions.V8;
using Raven.Client;
using Raven.Client.Documents.Indexes.Spatial;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Documents;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.ServerWide;
using Raven.Server.Documents.Indexes.Static.Spatial;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Raven.Server.Documents.Queries.Timings;
using Raven.Server.Documents.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Spatial4n.Core.Distance;
using ExpressionType = System.Linq.Expressions.ExpressionType;
using Raven.Server.Documents.Patch.V8;

namespace Raven.Server.Documents.Patch
{
    public partial class ScriptRunner
    {
        public static unsafe DateTime GetDateArg(InternalHandle arg, string signature, string argName)
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

        private static DateTime GetTimeSeriesDateArg(InternalHandle arg, string signature, string argName)
        {
            if (arg.IsDate)
                return arg.AsDate;

            if (arg.IsStringEx == false)
                throw new ArgumentException($"{signature} : {argName} must be of type 'DateInstance' or a DateTime string. {GetTypes(arg)}");

            return TimeSeriesRetriever.ParseDateTime(arg.AsString);
        }

        private static string GetTypes(InternalHandle value) => $"V8Type({value.ValueType}) .NETType({value.GetType().Name})";

        public partial class SingleRun
        {
            private PoolWithLevels<V8EngineEx>.PooledValue _scriptEngineV8Pooled;
            public V8EngineEx ScriptEngineExV8 => _scriptEngineV8Pooled.Value;
            public V8Engine ScriptEngineV8 => _scriptEngineV8Pooled.Value;
            public JavaScriptUtilsV8 JsUtilsV8;

            private V8EngineEx.ContextEx _contextExV8; 

            private Exception _lastException;

            public Exception LastException
            {
                get => _lastException;
                set
                {
                    _lastException = value;
                }
            }

            public void InitializeV8()
            {
                var poolOfEngines = V8EngineEx.GetPool(_jsOptions);
                _scriptEngineV8Pooled = poolOfEngines.GetValue();
                ScriptEngineHandle = ScriptEngineExV8;

                JsUtilsV8 = new JavaScriptUtilsV8(_runnerBase, ScriptEngineExV8);
                JsUtilsBase = JsUtilsV8;
            }
            
            public void InitializeLockedV8()
            {
                _contextExV8 = ScriptEngineExV8.CreateAndSetContextEx(_jsOptions, this);
            }
            
            private void SetContextV8()
            {
                ScriptEngineExV8.Context = _contextExV8;
            }
            
            public void DisposeV8()
            {
                _contextExV8.Dispose();
                _scriptEngineV8Pooled.Dispose();
            }
            
            private (string Id, BlittableJsonReaderObject Doc) GetIdAndDocFromArg(InternalHandle docArg, string signature)
            {
                if (docArg.BoundObject is BlittableObjectInstanceV8 doc)
                    return (doc.DocumentId, doc.Blittable);

                if (docArg.IsStringEx)
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
                if (docArg.BoundObject is BlittableObjectInstanceV8 doc)
                    return doc.DocumentId;

                if (docArg.IsStringEx)
                {
                    var id = docArg.AsString;
                    return id;
                }

                throw new InvalidOperationException($"{signature}: 'doc' must be a string argument (the document id) or the actual document instance itself. {GetTypes(docArg)}");
            }

            private static string GetStringArg(InternalHandle jsArg, string signature, string argName)
            {
                if (jsArg.IsStringEx == false)
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
                        if (jsItem.IsNumberOrIntEx == false)
                            throw new ArgumentException($"{signature}: The values argument must be an array of numbers, but got {jsItem.ValueType} key({i}) value({jsItem})");
                        array[i] = jsItem.AsDouble;
                    }
                }
            }

            private InternalHandle TimeSeriesV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    AssertValidDatabaseContext(_timeSeriesSignature);

                    if (args.Length != 2)
                        throw new ArgumentException($"{_timeSeriesSignature}: This method requires 2 arguments but was called with {args.Length}");

                    var obj = ScriptEngineV8.CreateObject();
                    obj.SetProperty("append", AppendTimeSeries.V8.Item.Clone());
                    obj.SetProperty("increment", IncrementTimeSeries.V8.Item.Clone());
                    obj.SetProperty("delete", DeleteRangeTimeSeries.V8.Item.Clone());
                    obj.SetProperty("get", GetRangeTimeSeries.V8.Item.Clone());
                    obj.SetProperty("getStats", GetStatsTimeSeries.V8.Item.Clone());
                    obj.SetProperty("doc", args[0]);
                    obj.SetProperty("name", args[1]);

                    return obj;
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle GetStatsTimeSeriesV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
            {
                try
                {
                    using (var document = self.GetProperty("doc"))
                    using (var name = self.GetProperty("name"))
                    {
                        var (id, doc) = GetIdAndDocFromArg(document, _timeSeriesSignature);

                        string timeSeries = GetStringArg(name, _timeSeriesSignature, "name");
                        var stats = _database.DocumentsStorage.TimeSeriesStorage.Stats.GetStats(_docsCtx, id, timeSeries);

                        var tsStats = ScriptEngineV8.CreateObject();
                        tsStats.SetProperty(nameof(stats.Start), engine.CreateValue(stats.Start));
                        tsStats.SetProperty(nameof(stats.End), engine.CreateValue(stats.End));
                        tsStats.SetProperty(nameof(stats.Count), engine.CreateValue(stats.Count));
                        return tsStats;
                    }
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle AppendTimeSeriesV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
            {
                try
                {
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

                            GetTimeSeriesValuesV8(jsValues, ref valuesBuffer, signature, out var values);

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
                    }
                    return InternalHandle.Empty;
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle IncrementTimeSeriesV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
            {
                try
                {
                    using (var document = self.GetProperty("doc"))
                    using (var name = self.GetProperty("name"))
                    {
                        AssertValidDatabaseContext("timeseries(doc, name).increment");

                        const string signature1Args = "timeseries(doc, name).increment(values)";
                        const string signature2Args = "timeseries(doc, name).increment(timestamp, values)";

                        string signature;
                        DateTime timestamp;
                        InternalHandle valuesArg;

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
                            GetTimeSeriesValuesV8(valuesArg, ref valuesBuffer, signature, out var values);

                            var tss = _database.DocumentsStorage.TimeSeriesStorage;
                            var newSeries = tss.Stats.GetStats(_docsCtx, id, timeSeries).Count == 0;

                            if (newSeries)
                            {
                                DocumentTimeSeriesToUpdate ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                                DocumentTimeSeriesToUpdate.Add(id);
                            }

                            var toIncrement = new TimeSeriesOperation.IncrementOperation {Values = valuesBuffer, ValuesLength = values.Length, Timestamp = timestamp};

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
                                    ["Name"] = timeSeries, ["Timestamp"] = timestamp, ["Values"] = values.ToArray().Cast<object>(), ["Created"] = newSeries
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
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private void GetTimeSeriesValuesV8(InternalHandle jsValues, ref double[] valuesBuffer, string signature, out Memory<double> values)
            {
                if (jsValues.IsArray)
                {
                    valuesBuffer = ArrayPool<double>.Shared.Rent((int)jsValues.ArrayLength);
                    FillDoubleArrayFromJsArray(valuesBuffer, jsValues, signature);
                    values = new Memory<double>(valuesBuffer, 0, (int)jsValues.ArrayLength);
                }
                else if (jsValues.IsNumberOrIntEx)
                {
                    valuesBuffer = ArrayPool<double>.Shared.Rent(1);
                    valuesBuffer[0] = jsValues.AsDouble;
                    values = new Memory<double>(valuesBuffer, 0, 1);
                }
                else
                {
                    throw new ArgumentException($"{signature}: The values should be an array but got {GetTypes(jsValues)}");
                }
            }

            private InternalHandle DeleteRangeTimeSeriesV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
            {
                try
                {
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
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle GetRangeTimeSeriesV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args)
            {
                try
                {
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

                        var entries = ScriptEngineV8.CreateArray(Array.Empty<InternalHandle>());
                        var noEntries = true;
                        foreach (var singleResult in reader.AllValues())
                        {
                            Span<double> valuesSpan = singleResult.Values.Span;
                            var jsSpanItems = new InternalHandle[valuesSpan.Length];
                            for (int i = 0; i < valuesSpan.Length; i++)
                            {
                                jsSpanItems[i] = ScriptEngineV8.CreateValue(valuesSpan[i]);
                            }

                            using (var entry = ScriptEngineV8.CreateObject())
                            {
                                entry.SetProperty(nameof(TimeSeriesEntry.Timestamp), engine.CreateValue(singleResult.Timestamp.GetDefaultRavenFormat(isUtc: true)));
                                entry.SetProperty(nameof(TimeSeriesEntry.Tag), singleResult.Tag == null ? engine.CreateNullValue() : engine.CreateValue(singleResult.Tag.ToString()));
                                entry.SetProperty(nameof(TimeSeriesEntry.Values), ScriptEngineV8.CreateArrayWithDisposal(jsSpanItems));
                                entry.SetProperty(nameof(TimeSeriesEntry.IsRollup), engine.CreateValue(singleResult.Type == SingleResultType.RolledUp));
                                
                                using (var jsResPush = entries.StaticCall("push", entry))
                                    jsResPush.ThrowOnError();
                                if (noEntries)
                                    noEntries = false;
                            }

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
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
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
                    case JSValueType.NumberObject:
                    case JSValueType.Int32:
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
                            case JSValueType.NumberObject:
                            case JSValueType.Int32:
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

            private InternalHandle Raven_MaxV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    GenericSortTwoElementArray(args);
                    return args[1];
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle Raven_MinV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    GenericSortTwoElementArray(args);
                    return args[0].IsNull ? args[1] : args[0];
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle IncludeDocV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
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
                                if (args[0].IsStringEx)
                                    IncludeDocV8(engine, isConstructCall, self, args);
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

                    InternalHandle jsRes = InternalHandle.Empty;
                    return jsRes.Set(self);
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle IncludeCompareExchangeValueV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
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
                                if (args[0].IsStringEx)
                                    IncludeCompareExchangeValueV8(engine, isConstructCall, self, args);
                            }
                        }
                        args[0] = jsArray;
                        return jsRes.Set(self);
                    }

                    if (args[0].IsStringEx == false)
                        throw new InvalidOperationException("includes.cmpxchg(key) must be called with an string or string array argument");

                    var key = args[0].AsString;

                    if (CompareExchangeValueIncludes == null)
                        CompareExchangeValueIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                    CompareExchangeValueIncludes.Add(key);

                    return jsRes.Set(self);
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle GetLastModifiedV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    if (args.Length != 1)
                        throw new InvalidOperationException("lastModified(doc) must be called with a single argument");

                    if (args[0].IsNull || args[0].IsUndefined)
                        return args[0];

                    if (args[0].IsObject == false)
                        throw new InvalidOperationException("lastModified(doc) must be called with an object argument");

                    if (args[0].BoundObject is BlittableObjectInstanceV8 doc)
                    {
                        if (doc.LastModified == null)
                            return InternalHandle.Empty;

                        // we use UTC because last modified is in UTC
                        var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
                        var time = doc.LastModified.Value.Subtract(epoch)
                            .TotalMilliseconds;
                        return ScriptEngineV8.CreateValue(time);
                    }
                    return InternalHandle.Empty;
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle Spatial_DistanceV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    if (args.Length < 4 && args.Length > 5)
                        throw new ArgumentException("Called with expected number of arguments, expected: spatial.distance(lat1, lng1, lat2, lng2, kilometers | miles | cartesian)");

                    for (int i = 0; i < 4; i++)
                    {
                        if (args[i].IsNumberOrIntEx == false)
                            return InternalHandle.Empty;
                    }

                    var lat1 = args[0].AsDouble;
                    var lng1 = args[1].AsDouble;
                    var lat2 = args[2].AsDouble;
                    var lng2 = args[3].AsDouble;

                    var units = SpatialUnits.Kilometers;
                    if (args.Length > 4 && args[4].IsStringEx)
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
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle OutputDebugV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    InternalHandle jsRes = InternalHandle.Empty;
                    if (DebugMode == false)
                        return jsRes.Set(self);

                    InternalHandle obj = args[0];

                    DebugOutput.Add(GetDebugValue(obj, false));
                    return jsRes.Set(self);
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private string GetDebugValue(InternalHandle obj, bool recursive)
            {
                if (obj.IsStringEx)
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
                    if (obj.BoundObject is BlittableObjectInstanceV8 boi && boi.Changed == false)
                    {
                        return boi.Blittable.ToString();
                    }

                    using (var blittable = JsBlittableBridgeV8.Translate(_jsonCtx, ScriptEngineV8, obj, isRoot: !recursive))
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

            public InternalHandle ExplodeArgsV8(V8Engine engine, bool isConstructCall, InternalHandle self, InternalHandle[] args)
            {
                if (args.Length != 2)
                    throw new InvalidOperationException("Raven_ExplodeArgs(this, args) - must be called with 2 arguments");
                if (args[1].IsObject && args[1].BoundObject is BlittableObjectInstanceV8 boi)
                {
                    SetArgsV8();
                    return self;
                }
                if (args[1].IsNull || args[1].IsUndefined)
                    return self;// noop
                throw new InvalidOperationException("Raven_ExplodeArgs(this, args) second argument must be BlittableObjectInstance");
            }

            public InternalHandle PutDocumentV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    string changeVector = null;

                    if (args.Length != 2 && args.Length != 3)
                        throw new InvalidOperationException("put(id, doc, changeVector) must be called with called with 2 or 3 arguments only");
                    AssertValidDatabaseContext("put document");
                    AssertNotReadOnly();
                    if (args[0].IsStringEx == false && args[0].IsNull == false && args[0].IsUndefined == false)
                        AssertValidId();

                    var id = args[0].IsNull || args[0].IsUndefined ? null : args[0].AsString;

                    if (args[1].IsObject == false)
                        throw new InvalidOperationException(
                            $"Created document must be a valid object which is not null or empty. Document ID: '{id}'.");

                    PutOrDeleteCalled = true;

                    if (args.Length == 3)
                        if (args[2].IsStringEx)
                            changeVector = args[2].AsString;
                        else if (args[2].IsNull == false && args[0].IsUndefined == false)
                            throw new InvalidOperationException(
                                $"The change vector must be a string or null. Document ID: '{id}'.");

                    BlittableJsonReaderObject reader = null;
                    try
                    {
                        reader = JsBlittableBridgeV8.Translate(_jsonCtx, ScriptEngineV8, args[1], usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk);

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
                    _lastException = e;
                    return engine.CreateError(e.Message, JSValueType.ExecutionError);
                }
            }

            public InternalHandle DeleteDocumentV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    if (args.Length != 1 && args.Length != 2)
                        throw new InvalidOperationException("delete(id, changeVector) must be called with at least one parameter");

                    if (args[0].IsStringEx == false)
                        throw new InvalidOperationException("delete(id, changeVector) id argument must be a string");

                    var id = args[0].AsString;
                    string changeVector = null;

                    if (args.Length == 2 && args[1].IsStringEx)
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
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle IncludeRevisionsV8(V8Engine engine, bool isConstructCall, InternalHandle self, InternalHandle[] args)
            {
                if (args == null)
                    return ScriptEngineV8.CreateNullValue();

                IncludeRevisionsChangeVectors ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);

                foreach (InternalHandle arg in args)
                {
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
                            InternalHandle jsArray = arg;
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

                return ScriptEngineV8.CreateNullValue();
            }

            private InternalHandle LoadDocumentByPathV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    using (_loadScope = _loadScope?.Start() ?? _scope?.For(nameof(QueryTimingsScope.Names.Load)))
                    {
                        AssertValidDatabaseContext("loadPath");

                        if (args.Length != 2 ||
                            (args[0].IsNull == false && args[0].IsUndefined == false && args[0].IsObject == false)
                            || args[1].IsStringEx == false)
                            throw new InvalidOperationException("loadPath(doc, path) must be called with a document and path");

                        if (args[0].IsNull || args[1].IsUndefined)
                            return args[0];

                        if (args[0].BoundObject is BlittableObjectInstanceV8 b)
                        {
                            var path = args[1].AsString;
                            if (_documentIds == null)
                                _documentIds = new HashSet<string>();

                            _documentIds.Clear();
                            IncludeUtil.GetDocIdFromInclude(b.Blittable, path, _documentIds, _database.IdentityPartsSeparator);
                            if (path.IndexOf("[]", StringComparison.InvariantCulture) != -1) // array
                                return ScriptEngineV8.FromObject(_documentIds.Select(LoadDocumentInternalV8).ToList());
                            if (_documentIds.Count == 0)
                                return ScriptEngineV8.CreateNullValue();

                            return LoadDocumentInternalV8(_documentIds.First());
                        }

                        throw new InvalidOperationException("loadPath(doc, path) must be called with a valid document instance, but got a JS object instead");
                    }
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle CompareExchangeV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    AssertValidDatabaseContext("cmpxchg");

                    if (args.Length != 1 && args.Length != 2 || args[0].IsStringEx == false)
                        throw new InvalidOperationException("cmpxchg(key) must be called with a single string argument");

                    return CmpXchangeInternalV8(CompareExchangeKey.GetStorageKey(_database.Name, args[0].AsString));
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle LoadDocumentV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    using (_loadScope = _loadScope?.Start() ?? _scope?.For(nameof(QueryTimingsScope.Names.Load)))
                    {
                        AssertValidDatabaseContext("load");

                        if (args.Length != 1)
                            throw new InvalidOperationException($"load(id | ids) must be called with a single string argument");

                        if (args[0].IsNull || args[0].IsUndefined)
                            return args[0];

                        if (args[0].IsArray)
                        {
                            var results = ScriptEngineV8.CreateArray(Array.Empty<InternalHandle>());
                            var jsArray = args[0];
                            int arrayLength = jsArray.ArrayLength;
                            for (int i = 0; i < arrayLength; ++i)
                            {
                                using (var jsItem = jsArray.GetProperty(i))
                                {
                                    if (jsItem.IsStringEx == false)
                                        throw new InvalidOperationException("load(ids) must be called with a array of strings, but got " + jsItem.ValueType + " - " + jsItem.ToString());
                                    using (var result = LoadDocumentInternalV8(jsItem.AsString))
                                    using (var jsResPush = results.Call("push", InternalHandle.Empty, result))
                                        jsResPush.ThrowOnError(); // TODO check if is needed here
                                }
                            }
                            return results;
                        }

                        if (args[0].IsStringEx == false)
                            throw new InvalidOperationException("load(id | ids) must be called with a single string or array argument");

                        return LoadDocumentInternalV8(args[0].AsString);
                    }
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle GetCounterV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    return GetCounterInternal(args);
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle GetCounterRawV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    return GetCounterInternal(args, true);
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle GetCounterInternal(InternalHandle[] args, bool raw = false)
            {
                var signature = raw ? "counterRaw(doc, name)" : "counter(doc, name)";
                AssertValidDatabaseContext(signature);

                if (args.Length != 2)
                    throw new InvalidOperationException($"{signature} must be called with exactly 2 arguments");

                string id;
                if (args[0].BoundObject is BlittableObjectInstanceV8 doc)
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
                    return InternalHandle.Empty;
                }

                if (raw == false)
                {
                    var counterValue = ScriptEngineV8.CreateValue(_database.DocumentsStorage.CountersStorage.GetCounterValue(_docsCtx, id, name)?.Value ?? null);

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

                var rawValues = ScriptEngineV8.CreateObject();
                foreach (var partialValue in _database.DocumentsStorage.CountersStorage.GetCounterPartialValues(_docsCtx, id, name))
                {
                    using (var jsPartialValue = ScriptEngineV8.CreateValue(partialValue.PartialValue))
                        rawValues.FastAddProperty(partialValue.ChangeVector, jsPartialValue, true, false, false);
                }

                return rawValues;
            }

            private InternalHandle IncrementCounterV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    AssertValidDatabaseContext("incrementCounter");

                    if (args.Length < 2 || args.Length > 3)
                    {
                        ThrowInvalidIncrementCounterArgs(args);
                    }

                    var signature = args.Length == 2 ? "incrementCounter(doc, name)" : "incrementCounter(doc, name, value)";

                    BlittableJsonReaderObject docBlittable = null;
                    string id = null;

                    if (args[0].BoundObject is BlittableObjectInstanceV8 doc)
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
                        if (args[2].IsNumberOrIntEx == false)
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
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle DeleteCounterV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    AssertValidDatabaseContext("deleteCounter");

                    if (args.Length != 2)
                    {
                        ThrowInvalidDeleteCounterArgs();
                    }

                    string id = null;
                    BlittableJsonReaderObject docBlittable = null;

                    if (args[0].BoundObject is BlittableObjectInstanceV8 doc)
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

                    return engine.CreateValue(true);
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle InvokeTimeSeriesFunctionV8(string name, InternalHandle[] args)
            {
                return InvokeTimeSeriesFunction(name, JsHandle.FromArray(args)).V8.Item;
            }

            private static void ThrowInvalidIncrementCounterArgs(InternalHandle[] args)
            {
                throw new InvalidOperationException($"There is no overload of method 'incrementCounter' that takes {args.Length} arguments." +
                                                    "Supported overloads are : 'incrementCounter(doc, name)' , 'incrementCounter(doc, name, value)'");
            }

            private InternalHandle ThrowOnLoadDocumentV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    throw new MissingMethodException("The method LoadDocumentV8 was renamed to 'load'");
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle ThrowOnPutDocumentV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    throw new MissingMethodException("The method PutDocumentV8 was renamed to 'put'");
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle ThrowOnDeleteDocumentV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    throw new MissingMethodException("The method DeleteDocumentV8 was renamed to 'del'");
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle ConvertJsTimeToTimeSpanStringV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    if (args.Length != 1 || args[0].IsNumberOrIntEx == false)
                        throw new InvalidOperationException("convertJsTimeToTimeSpanString(ticks) must be called with a single long argument");

                    var ticks = Convert.ToInt64(args[0].AsDouble) * 10000;

                    var asTimeSpan = new TimeSpan(ticks);

                    return engine.CreateValue(asTimeSpan.ToString());
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle ConvertToTimeSpanStringV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    if (args.Length == 1)
                    {
                        if (args[0].IsNumberOrIntEx == false)
                            throw new InvalidOperationException("convertToTimeSpanString(ticks) must be called with a single long argument");

                        var ticks = Convert.ToInt64(args[0].AsDouble);
                        var asTimeSpan = new TimeSpan(ticks);
                        return engine.CreateValue(asTimeSpan.ToString());
                    }

                    if (args.Length == 3)
                    {
                        if (args[0].IsNumberOrIntEx == false || args[1].IsNumberOrIntEx == false || args[2].IsNumberOrIntEx == false)
                            throw new InvalidOperationException("convertToTimeSpanString(hours, minutes, seconds) must be called with integer values");

                        var hours = Convert.ToInt32(args[0].AsDouble);
                        var minutes = Convert.ToInt32(args[1].AsDouble);
                        var seconds = Convert.ToInt32(args[2].AsDouble);

                        var asTimeSpan = new TimeSpan(hours, minutes, seconds);
                        return engine.CreateValue(asTimeSpan.ToString());
                    }

                    if (args.Length == 4)
                    {
                        if (args[0].IsNumberOrIntEx == false || args[1].IsNumberOrIntEx == false || args[2].IsNumberOrIntEx == false || args[3].IsNumberOrIntEx == false)
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
                        if (args[0].IsNumberOrIntEx == false || args[1].IsNumberOrIntEx == false || args[2].IsNumberOrIntEx == false || args[3].IsNumberOrIntEx == false || args[4].IsNumberOrIntEx == false)
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
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle CompareDatesV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
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

                    return engine.CreateValue(
                        binaryOperationType switch
                        {
                            ExpressionType.Subtract => (date1 - date2).ToString(),
                            ExpressionType.GreaterThan => date1 > date2,
                            ExpressionType.GreaterThanOrEqual => date1 >= date2,
                            ExpressionType.LessThan => date1 < date2,
                            ExpressionType.LessThanOrEqual => date1 <= date2,
                            ExpressionType.Equal => date1 == date2,
                            ExpressionType.NotEqual => date1 != date2,
                            _ => throw new InvalidOperationException($"compareDates(date1, date2, binaryOp) : unsupported binary operation '{binaryOperationType}'")
                        }
                    );
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private unsafe InternalHandle ToStringWithFormatV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
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
                        return engine.CreateValue(format != null ?
                            date.ToString(format, cultureInfo) :
                            date.ToString(cultureInfo));
                    }

                    if (args[0].IsNumberOrIntEx)
                    {
                        var num = args[0].AsDouble;
                        return engine.CreateValue(format != null ?
                            num.ToString(format, cultureInfo) :
                            num.ToString(cultureInfo));
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
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle StartsWithV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    if (args.Length != 2 || args[0].IsStringEx == false || args[1].IsStringEx == false)
                        throw new InvalidOperationException("startsWith(text, contained) must be called with two string parameters");

                    return engine.CreateValue(args[0].AsString.StartsWith(args[1].AsString, StringComparison.OrdinalIgnoreCase));
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle EndsWithV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    if (args.Length != 2 || args[0].IsStringEx == false || args[1].IsStringEx == false)
                        throw new InvalidOperationException("endsWith(text, contained) must be called with two string parameters");

                    return engine.CreateValue(args[0].AsString.EndsWith(args[1].AsString, StringComparison.OrdinalIgnoreCase));
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle RegexV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    if (args.Length != 2 || args[0].IsStringEx == false || args[1].IsStringEx == false)
                        throw new InvalidOperationException("regex(text, regex) must be called with two string parameters");

                    var regex = _regexCache.Get(args[1].AsString);

                    return engine.CreateValue(regex.IsMatch(args[0].AsString));
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle ScalarToRawStringV8(V8Engine engine, bool isConstructCall, InternalHandle self2, params InternalHandle[] args) // callback
            {
                try
                {
                    if (args.Length != 2)
                        throw new InvalidOperationException("scalarToRawString(document, lambdaToField) may be called on with two parameters only");

                    var firstParam = args[0];
                    if (firstParam.IsBinder && firstParam.BoundObject is BlittableObjectInstanceV8 selfInstance)
                    {
                        // we don't have access to declaration of the second parameter, so we expect it to contain the property name, but not the lambda expression as in Jint vase
                        // TODO [shlomo] RavenDB-18121
                        var secondParam = args[1];
                        if (secondParam.IsString)
                        {
                            string propName = secondParam.AsString;

                            IBlittableObjectProperty existingValue = default;
                            if (selfInstance.TryGetValue(propName, out existingValue, out var isDeleted) && existingValue != null && !isDeleted)
                            {
                                if (existingValue.Changed)
                                {
                                    return existingValue.ValueHandle.V8.Item;
                                }
                            }

                            var propertyIndex = selfInstance.Blittable.GetPropertyIndex(propName);

                            if (propertyIndex == -1)
                            {
                                return selfInstance.EngineV8.CreateObject();
                            }

                            BlittableJsonReaderObject.PropertyDetails propDetails = new BlittableJsonReaderObject.PropertyDetails();
                            selfInstance.Blittable.GetPropertyByIndex(propertyIndex, ref propDetails);
                            var value = propDetails.Value;

                            var engineEx = (V8EngineEx)engine;
                            switch (propDetails.Token & BlittableJsonReaderBase.TypesMask)
                            {
                                case BlittableJsonToken.Null:
                                    return engine.CreateNullValue();
                                case BlittableJsonToken.Boolean:
                                    return engine.CreateValue((bool)value);
                                case BlittableJsonToken.Integer:
                                    return engine.CreateValue((long)value); //engineEx.CreateObjectBinder(value); // TODO [shlomo] in Jint we create wrapper object, maybe here we need to do the same - the question is to which type the value should be cast 
                                 case BlittableJsonToken.LazyNumber:
                                    return engineEx.CreateObjectBinder((LazyNumberValue)value);
                                case BlittableJsonToken.String:
                                    return engineEx.CreateObjectBinder((LazyStringValue)value);
                                case BlittableJsonToken.CompressedString:
                                    return engineEx.CreateObjectBinder((LazyCompressedStringValue)value);
                                default:
                                    throw new InvalidOperationException("scalarToRawString(document, lambdaToField) lambda to field must return either raw numeric or raw string types");
                            }
                        }
                        else if (secondParam.IsFunction)
                        {
                            var res = secondParam.StaticCall(firstParam);
                            if (res.IsUndefined)
                            {
                                return selfInstance.EngineV8.CreateObject();
                            }

                            return res;
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
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            public InternalHandle GetMetadataV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    return JsUtilsV8 != null ? JsUtilsV8.GetMetadata(engine, isConstructCall, self, args) : DummyJsCallbackV8(engine, isConstructCall, self, args);
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            public InternalHandle GetDocumentIdV8(V8Engine engine, bool isConstructCall, InternalHandle self, params InternalHandle[] args) // callback
            {
                try
                {
                    return JsUtilsV8 != null ? JsUtilsV8.GetDocumentId(engine, isConstructCall, self, args) : DummyJsCallbackV8(engine, isConstructCall, self, args);
                }
                catch (Exception e) 
                {
                    _lastException = e;
                    return engine.CreateError(e.ToString(), JSValueType.ExecutionError);
                }
            }

            private InternalHandle CmpXchangeInternalV8(string key)
            {
                if (string.IsNullOrEmpty(key))
                    return InternalHandle.Empty;

                using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var value = _database.ServerStore.Cluster.GetCompareExchangeValue(ctx, key).Value;
                    if (value == null)
                        return ScriptEngineV8.CreateNullValue();

                    using (var jsValue = JsUtilsV8.TranslateToJs(_jsonCtx, value.Clone(_jsonCtx), true))
                        return jsValue.GetProperty(Constants.CompareExchange.ObjectFieldName);
                }
            }

            private InternalHandle LoadDocumentInternalV8(string id)
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

                return JsUtilsV8.TranslateToJs(_jsonCtx, document, true);
            }
            public object TranslateToJsV8(JsonOperationContext context, object o, bool keepAlive = false)
            {
                return JsUtilsV8.TranslateToJs(context, o, keepAlive: keepAlive);
            }

            private JavaScriptException CreateFullError(V8Exception e)
            {
                var jsException = new JavaScriptException(e.Message, e);
                return jsException;
            }

            private void SetArgsV8()
            {
                if (_args.Length > 1 && _args[1].Object is BlittableObjectInstanceV8 boi)
                {
                    var global = ScriptEngineV8.GlobalObject;
                    foreach (var propertyNameOrig in boi.EnumerateOwnPropertiesUnordered())
                    {
                        var desc = boi.GetOwnProperty(propertyNameOrig);
                        if (desc != null)
                        {
                            var valueNew = desc.Value;
                            var propertyName = "$" + propertyNameOrig;
                            if (global.HasProperty(propertyName))
                            {
                                using (var valuePrev = global.GetProperty(propertyName))
                                {
                                    if (ReferenceEquals(valuePrev.Object, valueNew.Object))
                                    {
                                        return; // ExplodeArgsOn can be called after SetArgs in ScriptRunner, in this case we can just skip repeated setting
                                    }
                                    else
                                    {
                                        var valueNewStr = ScriptEngineHandle.JsonStringify().V8.Item.StaticCall(valueNew);
                                        var valuePrevStr = ScriptEngineHandle.JsonStringify().V8.Item.StaticCall(valuePrev);
                                        throw new ArgumentException(
                                            $"Can't set argument '{propertyName}' as property on global object as it already exists with value {valuePrevStr}, new value {valueNewStr}");
                                    }
                                }
                            }

                            if (!global.SetProperty(propertyName, valueNew.Clone()))
                            {
                                throw new JavaScriptException($"Failed to set property {propertyName} on global object");
                            }
                        }
                    }
                }
            }
            
            private void DisposeArgsV8()
            {
                if (_args.Length > 1 && _args[1].Kind == JsHandleType.V8 && _args[1].Object is BlittableObjectInstanceV8 boi)
                {
                    var global = ScriptEngineV8.GlobalObject;
                    foreach (var propertyNameOrig in boi.EnumerateOwnPropertiesUnordered())
                    {
                        var propertyName = "$" + propertyNameOrig;
                        if (global.HasProperty(propertyName))
                        {
                            if (!global.DeleteProperty(propertyName))
                            {
                                throw new JavaScriptException($"Failed to delete property {propertyName} on global object");
                            }
                        }
                    }
                }
            }
            
            public object TranslateV8(JsonOperationContext context, object o)
            {
                return JsUtilsV8.TranslateToJs(context, o);
            }
        }
    }
}
