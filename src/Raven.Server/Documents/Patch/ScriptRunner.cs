using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
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
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.Config;
using Raven.Server.Documents.Indexes;
using Raven.Server.Extensions;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using JavaScriptException = Jint.Runtime.JavaScriptException;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunner
    {
        private readonly ConcurrentQueue<SingleRun> _cache = new ConcurrentQueue<SingleRun>();
        private readonly DocumentDatabase _db;
        private readonly RavenConfiguration _configuration;
        private readonly bool _enableClr;
        private readonly DateTime _creationTime;
        public readonly List<string> ScriptsSource = new List<string>();

        public long Runs;
        DateTime _lastRun;

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

        public ReturnRun GetRunner(out SingleRun run)
        {
            _lastRun = DateTime.UtcNow;
            if (_cache.TryDequeue(out run) == false)
                run = new SingleRun(_db, _configuration, this, ScriptsSource);
            Interlocked.Increment(ref Runs);
            return new ReturnRun(this, run);
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

        public class SingleRun
        {
            private readonly DocumentDatabase _database;
            private readonly RavenConfiguration _configuration;

            private readonly List<IDisposable> _disposables = new List<IDisposable>();
            private readonly ScriptRunner _runner;
            public readonly Engine ScriptEngine;
            private DocumentsOperationContext _docsCtx;
            private JsonOperationContext _jsonCtx;
            public PatchDebugActions DebugActions;
            public bool DebugMode;
            public List<string> DebugOutput;
            public bool PutOrDeleteCalled;
            public HashSet<string> Includes;
            private HashSet<string> _documentIds;
            public bool ReadOnly;
            public string OriginalDocumentId;
            public bool RefreshOriginalDocument;
            private readonly ConcurrentLruRegexCache _regexCache = new ConcurrentLruRegexCache(1024);

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
                        .Strict()
                        .AddObjectConverter(new JintGuidConverter())
                        .AddObjectConverter(new JintStringConverter())
                        .AddObjectConverter(new JintEnumConverter())
                        .AddObjectConverter(new JintDateTimeConverter())
                        .AddObjectConverter(new JintTimeSpanConverter())
                        .LocalTimeZone(TimeZoneInfo.Utc);

                });
                ScriptEngine.SetValue("output", new ClrFunctionInstance(ScriptEngine, OutputDebug));
                ObjectInstance consoleObject = new ObjectInstance(ScriptEngine);
                consoleObject.FastAddProperty("log", new ClrFunctionInstance(ScriptEngine, OutputDebug), false, false, false);
                ScriptEngine.SetValue("console", consoleObject);
                ScriptEngine.SetValue("include", new ClrFunctionInstance(ScriptEngine, IncludeDoc));
                ScriptEngine.SetValue("load", new ClrFunctionInstance(ScriptEngine, LoadDocument));
                ScriptEngine.SetValue("LoadDocument", new ClrFunctionInstance(ScriptEngine, ThrowOnLoadDocument));
                ScriptEngine.SetValue("loadPath", new ClrFunctionInstance(ScriptEngine, LoadDocumentByPath));
                ScriptEngine.SetValue("del", new ClrFunctionInstance(ScriptEngine, DeleteDocument));
                ScriptEngine.SetValue("DeleteDocument", new ClrFunctionInstance(ScriptEngine, ThrowOnDeleteDocument));
                ScriptEngine.SetValue("put", new ClrFunctionInstance(ScriptEngine, PutDocument));
                ScriptEngine.SetValue("PutDocument", new ClrFunctionInstance(ScriptEngine, ThrowOnPutDocument));
                ScriptEngine.SetValue("cmpxchg", new ClrFunctionInstance(ScriptEngine, CompareExchange));

                ScriptEngine.SetValue("getMetadata", new ClrFunctionInstance(ScriptEngine, GetMetadata));

                ScriptEngine.SetValue("id", new ClrFunctionInstance(ScriptEngine, GetDocumentId));
                ScriptEngine.SetValue("lastModified", new ClrFunctionInstance(ScriptEngine, GetLastModified));

                ScriptEngine.SetValue("startsWith", new ClrFunctionInstance(ScriptEngine, StartsWith));
                ScriptEngine.SetValue("endsWith", new ClrFunctionInstance(ScriptEngine, EndsWith));
                ScriptEngine.SetValue("regex", new ClrFunctionInstance(ScriptEngine, Regex));

                ScriptEngine.SetValue("Raven_ExplodeArgs", new ClrFunctionInstance(ScriptEngine, ExplodeArgs));
                ScriptEngine.SetValue("Raven_Min", new ClrFunctionInstance(ScriptEngine, Raven_Min));
                ScriptEngine.SetValue("Raven_Max", new ClrFunctionInstance(ScriptEngine, Raven_Max));

                ScriptEngine.SetValue("convertJsTimeToTimeSpanString", new ClrFunctionInstance(ScriptEngine, ConvertJsTimeToTimeSpanString));

                ScriptEngine.SetValue("toStringWithFormat", new ClrFunctionInstance(ScriptEngine, ToStringWithFormat));


                ScriptEngine.SetValue("scalarToRawString", new ClrFunctionInstance(ScriptEngine, ScalarToRawString));

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
            }

            private void GenericSortTwoElementArray(JsValue[] args, [CallerMemberName]string caller = null)
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
                    foreach (var pair in array.GetOwnProperties())
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
                    return new JsValue(jsTime);
                }
                return Undefined.Instance;
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
                AssertValidDatabaseContext();
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
                        //RavenDB-11391 This flag was added to cause attachment metadata table check & remove metadata properties if not necessary
                        nonPersistentFlags: NonPersistentDocumentFlags.ResolveAttachmentsConflict
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
                AssertValidDatabaseContext();
                AssertNotReadOnly();
                if (DebugMode)
                    DebugActions.DeleteDocument.Add(id);
                var result = _database.DocumentsStorage.Delete(_docsCtx, id, changeVector);

                if (RefreshOriginalDocument && string.Equals(OriginalDocumentId, id, StringComparison.OrdinalIgnoreCase))
                    RefreshOriginalDocument = false;

                return new JsValue(result != null);
            }

            private void AssertNotReadOnly()
            {
                if (ReadOnly)
                    throw new InvalidOperationException("Cannot make modifications in readonly context");
            }

            private void AssertValidDatabaseContext()
            {
                if (_docsCtx == null)
                    throw new InvalidOperationException("Unable to put documents when this instance is not attached to a database operation");
            }

            private JsValue GetDocumentId(JsValue self, JsValue[] args)
            {
                if (args.Length != 1)
                    throw new InvalidOperationException("id(doc) must be called with a single argument");

                if (args[0].IsNull() || args[0].IsUndefined())
                    return args[0];

                if (args[0].IsObject() == false)
                    throw new InvalidOperationException("id(doc) must be called with an object argument");

                var objectInstance = args[0].AsObject();

                if (objectInstance is BlittableObjectInstance doc && doc.DocumentId != null)
                    return new JsValue(doc.DocumentId);

                var jsValue = objectInstance.Get(Constants.Documents.Metadata.Key);
                // search either @metadata.@id or @id
                var metadata = jsValue.IsObject() == false ? objectInstance : jsValue.AsObject();
                var value = metadata.Get(Constants.Documents.Metadata.Id);
                if (value.IsString() == false)
                {
                    // search either @metadata.Id or Id
                    value = metadata.Get(Constants.Documents.Metadata.IdProperty);
                    if (value.IsString() == false)
                        return JsValue.Null;
                }
                return value;
            }



            private JsValue LoadDocumentByPath(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext();


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
                    IncludeUtil.GetDocIdFromInclude(b.Blittable, path, _documentIds);
                    if (path.IndexOf("[]", StringComparison.InvariantCulture) != -1) // array
                        return JsValue.FromObject(ScriptEngine, _documentIds.Select(LoadDocumentInternal).ToList());
                    if (_documentIds.Count == 0)
                        return JsValue.Null;

                    return LoadDocumentInternal(_documentIds.First());

                }

                throw new InvalidOperationException("loadPath(doc, path) must be called with a valid document instance, but got a JS object instead");
            }


            private JsValue GetMetadata(JsValue self, JsValue[] args)
            {
                if (args.Length != 1 || !(args[0].AsObject() is BlittableObjectInstance boi))
                    throw new InvalidOperationException("getMetadata(doc) must be called with a single entity argument");

                if (!(boi.Blittable[Constants.Documents.Metadata.Key] is BlittableJsonReaderObject metadata))
                    return JsValue.Null;

                metadata.Modifications = new DynamicJsonValue
                {
                    [Constants.Documents.Metadata.ChangeVector] = boi.ChangeVector,
                    [Constants.Documents.Metadata.Id] = boi.DocumentId,
                    [Constants.Documents.Metadata.LastModified] = boi.LastModified,
                };

                metadata = _jsonCtx.ReadObject(metadata, boi.DocumentId);

                return TranslateToJs(ScriptEngine, _jsonCtx, metadata);
            }

            private JsValue CompareExchange(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext();

                if (args.Length != 1 || args[0].IsString() == false)
                    throw new InvalidOperationException("cmpxchg(key) must be called with a single string argument");

                return CmpXchangeInternal(args[0].AsString());
            }

            private JsValue LoadDocument(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext();

                if (args.Length != 1)
                    throw new InvalidOperationException("load(id | ids) must be called with a single string argument");

                if (args[0].IsNull() || args[0].IsUndefined())
                    return args[0];

                if (args[0].IsArray())
                {
                    var results = (ArrayInstance)ScriptEngine.Array.Construct(Array.Empty<JsValue>());
                    var arrayInstance = args[0].AsArray();
                    foreach (var kvp in arrayInstance.GetOwnProperties())
                    {
                        if (kvp.Key == "length")
                            continue;
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

            private JsValue ThrowOnLoadDocument(JsValue self, JsValue[] args)
            {
                throw new MissingMethodException("The method LoadDocument was renamed to 'load'");
            }

            private JsValue ThrowOnPutDocument(JsValue self, JsValue[] args)
            {
                throw new MissingMethodException("The method PutDocument was renamed to 'put'");
            }

            private JsValue ThrowOnDeleteDocument(JsValue self, JsValue[] args)
            {
                throw new MissingMethodException("The method DeleteDocument was renamed to 'del'");
            }

            private static JsValue ConvertJsTimeToTimeSpanString(JsValue self, JsValue[] args)
            {
                if (args.Length != 1 || args[0].IsNumber() == false)
                    throw new InvalidOperationException("convertJsTimeToTimeSpanString(ticks) must be called with a single long argument");

                var ticks = Convert.ToInt64(args[0].AsNumber()) * 10000;

                var asTimeSpan = new TimeSpan(ticks);

                return new JsValue(asTimeSpan.ToString());
            }

            private static JsValue ToStringWithFormat(JsValue self, JsValue[] args)
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
                    throw new InvalidOperationException("startsWith(text, contained) must be called with two string paremters");

                return new JsValue(args[0].AsString().StartsWith(args[1].AsString(), StringComparison.OrdinalIgnoreCase));
            }

            private static JsValue EndsWith(JsValue self, JsValue[] args)
            {
                if (args.Length != 2 || args[0].IsString() == false || args[1].IsString() == false)
                    throw new InvalidOperationException("endsWith(text, contained) must be called with two string paremters");

                return new JsValue(args[0].AsString().EndsWith(args[1].AsString(), StringComparison.OrdinalIgnoreCase));
            }

            private JsValue Regex(JsValue self, JsValue[] args)
            {
                if (args.Length != 2 || args[0].IsString() == false || args[1].IsString() == false)
                    throw new InvalidOperationException("regex(text, regex) must be called with two string paremters");

                var regex = _regexCache.Get(args[1].AsString());

                return new JsValue(regex.IsMatch(args[0].AsString()));
            }

            private static JsValue ScalarToRawString(JsValue self2, JsValue[] args)
            {
                if (args.Length != 2)
                    throw new InvalidOperationException("scalarToRawString(document, lambdaToField) may be called on with two parameters only");


                JsValue firstParam = args[0];
                if (firstParam.IsObject() && args[0].AsObject() is BlittableObjectInstance selfInstance)
                {
                    JsValue secondParam = args[1];
                    if (secondParam.IsObject() && secondParam.AsObject() is ScriptFunctionInstance lambda)
                    {

                        var functionAst = lambda.GetFunctionAst();
                        var propName = functionAst.TryGetFieldFromSimpleLambdaExpression();

                        if (selfInstance.OwnValues.TryGetValue(propName, out var existingValue))
                        {
                            if (existingValue.Changed)
                            {
                                return existingValue.Value;
                            }
                        }

                        var propertyIndex = selfInstance.Blittable.GetPropertyIndex(propName);

                        if (propertyIndex == -1)
                        {
                            return new JsValue(new ObjectInstance(selfInstance.Engine)
                            {
                                Extensible = true
                            });
                        }

                        BlittableJsonReaderObject.PropertyDetails propDetails = new BlittableJsonReaderObject.PropertyDetails();
                        selfInstance.Blittable.GetPropertyByIndex(propertyIndex, ref propDetails);
                        var value = propDetails.Value;

                        switch (propDetails.Token & BlittableJsonReaderBase.TypesMask)
                        {
                            case BlittableJsonToken.Null:
                                return JsValue.Null;
                            case BlittableJsonToken.Boolean:
                                return new JsValue((bool)propDetails.Value);
                            case BlittableJsonToken.Integer:
                                return new JsValue(new ObjectWrapper(selfInstance.Engine, value));
                            case BlittableJsonToken.LazyNumber:
                                return new JsValue(new ObjectWrapper(selfInstance.Engine, value));
                            case BlittableJsonToken.String:
                                return new JsValue(new ObjectWrapper(selfInstance.Engine, value));
                            case BlittableJsonToken.CompressedString:
                                return new JsValue(new ObjectWrapper(selfInstance.Engine, value));
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
                var prefix = _database.Name + "/";
                using (_database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext ctx))
                using (ctx.OpenReadTransaction())
                {
                    var value = _database.ServerStore.Cluster.GetCompareExchangeValue(ctx, prefix + key).Value;
                    if (value == null)
                        return null;
                    // RavenDB-12067: Accessing a blittable whose context was disposed.
                    var jsValue = TranslateToJs(ScriptEngine, _jsonCtx, value.Clone(_jsonCtx));
                    return jsValue.AsObject().Get("Object");
                }
            }

            private JsValue LoadDocumentInternal(string id)
            {
                if (string.IsNullOrEmpty(id))
                    return JsValue.Undefined;
                if (DebugMode)
                    DebugActions.LoadDocument.Add(id);
                var document = _database.DocumentsStorage.Get(_docsCtx, id);
                return TranslateToJs(ScriptEngine, _jsonCtx, document);
            }

            public void DisposeClonedDocuments()
            {
                foreach (var disposable in _disposables)
                    disposable.Dispose();
                _disposables.Clear();
            }

            private JsValue[] _args = Array.Empty<JsValue>();
            private readonly JintPreventResolvingTasksReferenceResolver _refResolver = new JintPreventResolvingTasksReferenceResolver();

            public ScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, object[] args)
            {
                return Run(jsonCtx, docCtx, method, null, args);
            }

            public ScriptRunnerResult Run(JsonOperationContext jsonCtx, DocumentsOperationContext docCtx, string method, string documentId, object[] args)
            {
                _docsCtx = docCtx;
                _jsonCtx = jsonCtx ?? ThrowArgumentNull();

                Reset();
                OriginalDocumentId = documentId;

                if (_args.Length != args.Length)
                    _args = new JsValue[args.Length];
                for (var i = 0; i < args.Length; i++)
                    _args[i] = TranslateToJs(ScriptEngine, jsonCtx, args[i]);
                try
                {
                    var call = ScriptEngine.GetValue(method).TryCast<ICallable>();
                    var result = call.Call(Undefined.Instance, _args);
                    return new ScriptRunnerResult(this, result);
                }
                catch (JavaScriptException e)
                {
                    throw CreateFullError(e);
                }
                finally
                {
                    _refResolver.ExplodeArgsOn(null, null);
                    _docsCtx = null;
                    _jsonCtx = null;
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
                PutOrDeleteCalled = false;
                OriginalDocumentId = null;
                RefreshOriginalDocument = false;
                ScriptEngine.ResetCallStack();
                ScriptEngine.ResetStatementsCount();
                ScriptEngine.ResetTimeoutTicks();
            }

            public object Translate(JsonOperationContext context, object o)
            {
                return TranslateToJs(ScriptEngine, context, o);
            }

            private JsValue TranslateToJs(Engine engine, JsonOperationContext context, object o)
            {
                BlittableJsonReaderObject Clone(BlittableJsonReaderObject origin)
                {
                    if (ReadOnly)
                        return origin;

                    // RavenDB-8286
                    // here we need to make sure that we aren't sending a value to 
                    // the js engine that might be modified by the actions of the js engine
                    // for example, calling put() mgiht cause the original data to change 
                    // because we defrag the data that we looked at. We are handling this by
                    // ensuring that we have our own, safe, copy.
                    var cloned = origin.Clone(context);
                    _disposables.Add(cloned);
                    return cloned;
                }

                if (o is Tuple<Document, Lucene.Net.Documents.Document, IState> t)
                {
                    var d = t.Item1;
                    return new BlittableObjectInstance(engine, null, Clone(d.Data), d.Id, d.LastModified, d.ChangeVector)
                    {
                        LuceneDocument = t.Item2,
                        LuceneState = t.Item3
                    };
                }
                if (o is Document doc)
                {
                    return new BlittableObjectInstance(engine, null, Clone(doc.Data), doc.Id, doc.LastModified);
                }
                if (o is DocumentConflict dc)
                {
                    return new BlittableObjectInstance(engine, null, Clone(dc.Doc), dc.Id, dc.LastModified);
                }

                if (o is BlittableJsonReaderObject json)
                {
                    return new BlittableObjectInstance(engine, null, Clone(json), null, null);
                }

                if (o == null)
                    return Undefined.Instance;
                if (o is long lng)
                    return new JsValue(lng);
                if (o is BlittableJsonReaderArray bjra)
                {
                    var jsArray = engine.Array.Construct(Array.Empty<JsValue>());
                    var args = new JsValue[1];
                    for (var i = 0; i < bjra.Length; i++)
                    {
                        var value = TranslateToJs(engine, context, bjra[i]);
                        args[0] = value as JsValue ?? JsValue.FromObject(engine, value);
                        engine.Array.PrototypeObject.Push(jsArray, args);
                    }
                    return jsArray;
                }
                if (o is List<object> list)
                {
                    var jsArray = engine.Array.Construct(Array.Empty<JsValue>());
                    var args = new JsValue[1];
                    for (var i = 0; i < list.Count; i++)
                    {
                        var value = TranslateToJs(engine, context, list[i]);
                        args[0] = value as JsValue ?? JsValue.FromObject(engine, value);
                        engine.Array.PrototypeObject.Push(jsArray, args);
                    }
                    return jsArray;
                }
                // for admin
                if (o is RavenServer || o is DocumentDatabase)
                {
                    AssertAdminScriptInstance();
                    return JsValue.FromObject(engine, o);
                }
                if (o is ObjectInstance j)
                    return j;
                if (o is bool b)
                    return new JsValue(b);
                if (o is int integer)
                    return new JsValue(integer);
                if (o is double dbl)
                    return new JsValue(dbl);
                if (o is string s)
                    return new JsValue(s);
                if (o is LazyStringValue ls)
                    return new JsValue(ls.ToString());
                if (o is LazyCompressedStringValue lcs)
                    return new JsValue(lcs.ToString());
                if (o is LazyNumberValue lnv)
                    return new JsValue(lnv.ToString());
                if (o is JsValue js)
                    return js;
                throw new InvalidOperationException("No idea how to convert " + o + " to JsValue");
            }

            private void AssertAdminScriptInstance()
            {
                if (_runner._enableClr == false)
                    throw new InvalidOperationException("Unable to run admin scripts using this instance of the script runner, the EnableClr is set to false");
            }

            public object CreateEmptyObject()
            {
                return ScriptEngine.Object.Construct(Array.Empty<JsValue>());
            }

            public object Translate(ScriptRunnerResult result, JsonOperationContext context, JsBlittableBridge.IResultModifier modifier = null, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
            {
                var val = result.RawJsValue;
                if (val.IsString())
                    return val.AsString();
                if (val.IsBoolean())
                    return val.AsBoolean();
                if (val.IsObject())
                    return result.TranslateToObject(context, modifier, usageMode);
                if (val.IsNumber())
                    return val.AsNumber();
                if (val.IsNull() || val.IsUndefined())
                    return null;
                if (val.IsArray())
                    throw new InvalidOperationException("Returning arrays from scripts is not supported, only objects or primitves");
                throw new NotSupportedException("Unable to translate " + val.Type);
            }
        }

        public struct ReturnRun : IDisposable
        {
            private ScriptRunner _parent;
            private SingleRun _run;

            public ReturnRun(ScriptRunner parent, SingleRun run)
            {
                _parent = parent;
                _run = run;
            }

            public void Dispose()
            {
                if (_run == null)
                    return;
                _run.ReadOnly = false;
                _run.DebugMode = false;
                _run.DebugOutput?.Clear();
                _run.DebugActions?.Clear();
                _parent._cache.Enqueue(_run);
                _run = null;
                _parent = null;
            }
        }
    }
}
