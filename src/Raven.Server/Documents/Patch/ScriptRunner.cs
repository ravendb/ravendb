using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading;
using Jint;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Interop;
using Jint.Runtime.References;
using Lucene.Net.Store;
using Raven.Client;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using JavaScriptException = Jint.Runtime.JavaScriptException;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunner
    {
        private readonly ConcurrentQueue<SingleRun> _cache = new ConcurrentQueue<SingleRun>();
        private readonly DocumentDatabase _db;
        private readonly bool _enableClr;
        public readonly List<string> ScriptsSource = new List<string>();

        public long Runs;

        public ScriptRunner(DocumentDatabase db, bool enableClr)
        {
            _db = db;
            _enableClr = enableClr;
        }

        public void AddScript(string script)
        {
            ScriptsSource.Add(script);
        }

        public ReturnRun GetRunner(out SingleRun run)
        {
            if (_cache.TryDequeue(out run) == false)
                run = new SingleRun(_db, this, ScriptsSource);
            Interlocked.Increment(ref Runs);
            return new ReturnRun(this, run);
        }

        public void TryCompileScript(string script)
        {
            try
            {
                var engine = new Engine(options => { options.MaxStatements(1).LimitRecursion(1); });
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

            private readonly List<IDisposable> _disposables = new List<IDisposable>();
            private readonly ScriptRunner _runner;
            public readonly Engine ScriptEngine;
            private DocumentsOperationContext _context;
            public PatchDebugActions DebugActions;
            public bool DebugMode;
            public List<string> DebugOutput;
            public bool PutOrDeleteCalled;
            public HashSet<string> Includes;
            private HashSet<string> _documentIds;
            public bool ReadOnly;

            public class NullPropgationReferenceResolver : IReferenceResolver
            {
                public bool TryUnresolvableReference(Engine engine, Reference reference, out JsValue value)
                {
                    value = Null.Instance;
                    return true;
                }

                public bool TryPropertyReference(Engine engine, Reference reference, ref JsValue value)
                {
                    return value.IsNull() || value.IsUndefined();
                }

                public bool TryGetCallable(Engine engine, object callee, out JsValue value)
                {
                    value = new JsValue(new ClrFunctionInstance(engine, (thisObj, values) => thisObj));
                    return true;
                }

                public bool CheckCoercible(JsValue value)
                {
                    return true;
                }
            }

            public SingleRun(DocumentDatabase database, ScriptRunner runner, List<string> scriptsSource)
            {
                _database = database;
                _runner = runner;
                ScriptEngine = new Engine(options =>
                {
                    options.LimitRecursion(64)
                        .SetReferencesResolver(new NullPropgationReferenceResolver())
                        .MaxStatements(database.Configuration.Patching.MaxStepsForScript)
                        .Strict();
                });
                ScriptEngine.SetValue("output", new ClrFunctionInstance(ScriptEngine, OutputDebug));

                ScriptEngine.SetValue("include", new ClrFunctionInstance(ScriptEngine, IncludeDoc));
                ScriptEngine.SetValue("load", new ClrFunctionInstance(ScriptEngine, LoadDocument));
                ScriptEngine.SetValue("loadPath", new ClrFunctionInstance(ScriptEngine, LoadDocumentByPath));
                ScriptEngine.SetValue("del", new ClrFunctionInstance(ScriptEngine, DeleteDocument));
                ScriptEngine.SetValue("put", new ClrFunctionInstance(ScriptEngine, PutDocument));


                ScriptEngine.SetValue("id", new ClrFunctionInstance(ScriptEngine, GetDocumentId));
                ScriptEngine.SetValue("lastModified", new ClrFunctionInstance(ScriptEngine, GetLastModified));

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


            private JsValue GetLastModified(JsValue self, JsValue[] args)
            {
                if (args.Length != 1)
                    throw new InvalidOperationException("id(doc) must be called with a single argument");

                if (args[0].IsNull() || args[0].IsUndefined())
                    return args[0];

                if (args[0].IsObject() == false)
                    throw new InvalidOperationException("id(doc) must be called with an object argument");

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
                    using (var jsonObj = result.TranslateToObject(_context))
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

                if (DebugMode)
                    DebugActions.PutDocument.Add(id);

                using (var reader = JsBlittableBridge.Translate(_context, args[1].AsObject(), usageMode: BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var put = _database.DocumentsStorage.Put(_context, id, _context.GetLazyString(changeVector), reader);
                    return put.Id;
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
                var result = _database.DocumentsStorage.Delete(_context, id, changeVector);
                return new JsValue(result != null);
            }

            private void AssertNotReadOnly()
            {
                if (ReadOnly)
                    throw new InvalidOperationException("Cannot make modifications in readonly context");
            }

            private void AssertValidDatabaseContext()
            {
                if (_context == null)
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
                    return JsValue.Null;
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


            private JsValue LoadDocument(JsValue self, JsValue[] args)
            {
                AssertValidDatabaseContext();

                if (args.Length != 1 || args[0].IsString() == false)
                    throw new InvalidOperationException("load(id) must be called with a single string argument");

                return LoadDocumentInternal(args[0].AsString());
            }

            private JsValue LoadDocumentInternal(string id)
            {
                if (DebugMode)
                    DebugActions.LoadDocument.Add(id);
                var document = _database.DocumentsStorage.Get(_context, id);
                return TranslateToJs(ScriptEngine, _context, document);
            }

            public void DisposeClonedDocuments()
            {
                foreach (var disposable in _disposables)
                    disposable.Dispose();
                _disposables.Clear();
            }

            private JsValue[] _args = Array.Empty<JsValue>();

            public ScriptRunnerResult Run(DocumentsOperationContext ctx, string method, object[] args)
            {
                _context = ctx;
                Reset();
                if (_args.Length != args.Length)
                    _args = new JsValue[args.Length];
                for (var i = 0; i < args.Length; i++)
                    _args[i] = TranslateToJs(ScriptEngine, ctx, args[i]);
                JsValue result;
                try
                {
                    var call = ScriptEngine.GetValue(method).TryCast<ICallable>();
                    result = call.Call(Undefined.Instance, _args);
                }
                catch (JavaScriptException e)
                {
                    throw CreateFullError(ctx, e);
                }
                return new ScriptRunnerResult(this, result);
            }

            private static Client.Exceptions.Documents.Patching.JavaScriptException CreateFullError(DocumentsOperationContext ctx, JavaScriptException e)
            {
                string msg;
                if (e.Error.IsString())
                    msg = e.Error.AsString();
                else if (e.Error.IsObject())
                    msg = JsBlittableBridge.Translate(ctx, e.Error.AsObject()).ToString();
                else
                    msg = e.Error.ToString();

                msg = "At " + e.Column + ":" + e.LineNumber + Environment.NewLine + msg;
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
                    return new BlittableObjectInstance(engine, null, Clone(d.Data), d.Id, d.LastModified)
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
                    return new BlittableObjectInstance(engine, null, Clone(dc.Doc), dc.Id, dc.LastModified);
                if (o is BlittableJsonReaderObject json)
                    return new BlittableObjectInstance(engine, null, json, null, null);
                if (o == null)
                    return Undefined.Instance;
                if (o is long lng)
                    return new JsValue(lng);
                if (o is List<object> list)
                {
                    var jsArray = ScriptEngine.Array.Construct(Array.Empty<JsValue>());
                    var args = new JsValue[1];
                    for (var i = 0; i < list.Count; i++)
                    {
                        var value = TranslateToJs(ScriptEngine, context, list[i]);
                        args[0] = value as JsValue ?? JsValue.FromObject(ScriptEngine, value);
                        ScriptEngine.Array.PrototypeObject.Push(jsArray, args);
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
