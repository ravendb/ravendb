using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using Esprima;
using Jint.Native;
using Jint.Native.Object;
using Jint.Runtime.Descriptors.Specialized;
using Sparrow.Extensions;
using Raven.Client.Exceptions.Documents.Patching;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunner
    {
        private readonly DocumentDatabase _db;
        private readonly bool _enableClr;
        public readonly List<string> ScriptsSource = new List<string>();
        private const int DefaultStringSize = 50;

        public void AddScript(string script)
        {
            ScriptsSource.Add(script);
        }

        public class SingleRun
        {
            public List<string> DebugOutput;
            public bool DebugMode;
            public bool PutOrDeleteCalled;
            public PatchDebugActions DebugActions;

            public override string ToString()
            {
                return string.Join(Environment.NewLine, _runner.ScriptsSource);
            }

            private SingleRun()
            {
                // here just to get an instance that jurrasic
                // can use
            }

            public SingleRun(DocumentDatabase database, ScriptRunner runner, List<string> scriptsSource)
            {
                _database = database;
                _runner = runner;
                ScriptEngine = new Jint.Engine(options =>
                {
                    options.LimitRecursion(64)
                        .MaxStatements(1000) // TODO: Maxim make this configurable
                        .Strict();
                });
                //TODO: Maybe convert to ClrFunction for perf?
                ScriptEngine.SetValue("output", (Action<object>)OutputDebug);

                ScriptEngine.SetValue("load", (Func<string, object>)LoadDocument);
                ScriptEngine.SetValue("del", (Func<string, string, bool>)DeleteDocument);
                ScriptEngine.SetValue("put", (Func<string, object, string, string>)PutDocument);

                ScriptEngine.SetValue("id", (Func<object, string>)GetDocumentId);
                ScriptEngine.SetValue("lastModified", (Func<object, string>)GetLastModified);

                foreach (var script in scriptsSource)
                {
                    ScriptEngine.Execute(script);
                }
            }


            private string GetLastModified(object arg)
            {
                if (arg is BlittableObjectInstance doc)
                    return doc.LastModified?.GetDefaultRavenFormat();
                return null;
            }

            private void OutputDebug(object obj)
            {
                if (DebugMode == false)
                    return;

                if (obj is string str)
                {
                    DebugOutput.Add(str);
                }
                else if (obj is ObjectInstance json)
                {
                    var stringified = ScriptEngine.Json.Stringify(json, Array.Empty<JsValue>())
                        .AsString();
                    DebugOutput.Add(stringified);
                }
                else if (ReferenceEquals(obj, Undefined.Instance))
                {
                    DebugOutput.Add("undefined");
                }
                else if (obj == null || ReferenceEquals(obj, Null.Instance))
                {
                    DebugOutput.Add("null");
                }
                else
                {
                    DebugOutput.Add(obj.ToString());
                }
            }

            public string PutDocument(string id, object document, string changeVector)
            {
                PutOrDeleteCalled = true;
                AssertValidDatabaseContext();
                AssertNotReadOnly();
                var objectInstance = document as ObjectInstance;
                if (objectInstance == null)
                {
                    AssertValidDocumentObject(id);
                    return null;//never hit
                }
                AssertValidDatabaseContext();
                if (changeVector == "undefined")
                    changeVector = null;

                if (DebugMode)
                {
                    DebugActions.PutDocument.Add(id);
                }

                using (var reader = JsBlittableBridge.Translate(_context, objectInstance,
                    BlittableJsonDocumentBuilder.UsageMode.ToDisk))
                {
                    var put = _database.DocumentsStorage.Put(_context, id, _context.GetLazyString(changeVector), reader);
                    return put.Id;
                }
            }

            public bool DeleteDocument(string id, string changeVector)
            {
                PutOrDeleteCalled = true;
                AssertValidDatabaseContext();
                AssertNotReadOnly();
                if (DebugMode)
                {
                    DebugActions.DeleteDocument.Add(id);
                }
                var result = _database.DocumentsStorage.Delete(_context, id, changeVector);
                return result != null;

            }

            private void AssertNotReadOnly()
            {
                if (ReadOnly)
                    throw new InvalidOperationException("Cannot make modifications in readonly context");
            }

            private static void AssertValidDocumentObject(string id)
            {
                throw new InvalidOperationException(
                    $"Created document must be a valid object which is not null or empty. Document ID: '{id}'.");
            }

            private void AssertValidDatabaseContext()
            {
                if (_context == null)
                    throw new InvalidOperationException("Unable to put documents when this instance is not attached to a database operation");
            }

            private string GetDocumentId(object arg)
            {
                if (arg is BlittableObjectInstance doc)
                    return doc.DocumentId;
                return null;
            }

            private object LoadDocument(string id)
            {
                AssertValidDatabaseContext();

                if (DebugMode)
                {
                    DebugActions.LoadDocument.Add(id);
                }
                var document = _database.DocumentsStorage.Get(_context, id);
                return TranslateToJs(ScriptEngine, _context, document);
            }

            public bool ReadOnly;
            private readonly DocumentDatabase _database;
            private readonly ScriptRunner _runner;
            private DocumentsOperationContext _context;

            public int MaxSteps;
            public int CurrentSteps;
            public readonly Jint.Engine ScriptEngine;

            private static void ThrowTooManyLoopIterations() =>
                throw new TimeoutException("The scripts has run for too long and was aborted by the server");

            [MethodImpl(MethodImplOptions.AggressiveInlining)]
            public void OnStateLoopIteration()
            {
                CurrentSteps++;
                if (CurrentSteps < MaxSteps)
                    return;
                ThrowTooManyLoopIterations();
            }

            private readonly List<IDisposable> _disposables = new List<IDisposable>();

            public void DisposeClonedDocuments()
            {
                foreach (var disposable in _disposables)
                {
                    disposable.Dispose();
                }
                _disposables.Clear();
            }

            public ScriptRunnerResult Run(DocumentsOperationContext ctx, string method, object[] args)
            {
                _context = ctx;
                if (DebugMode)
                {
                    if (DebugOutput == null)
                        DebugOutput = new List<string>();
                    if (DebugActions == null)
                        DebugActions = new PatchDebugActions();
                }
                PutOrDeleteCalled = false;
                CurrentSteps = 0;
                MaxSteps = 1000; // TODO: Maxim make me configurable
                for (int i = 0; i < args.Length; i++)
                {
                    args[i] = TranslateToJs(ScriptEngine, ctx, args[i]);
                }
                var result = ScriptEngine.Invoke(method, args);
                return new ScriptRunnerResult(this, result);
            }


#if DEBUG
            static readonly HashSet<Type> ExpectedTypes = new HashSet<Type>
            {
                typeof(int),
                typeof(long),
                typeof(double),
                typeof(bool),
                typeof(string),
            };
#endif

            public object Translate(JsonOperationContext context, object o)
            {
                return TranslateToJs(ScriptEngine, context, o);
            }

            private object TranslateToJs(Jint.Engine engine, JsonOperationContext context, object o)
            {
                BlittableJsonReaderObject Clone(BlittableJsonReaderObject origin)
                {
                    if (ReadOnly)
                        return origin;
                    
                    // RavenDB-8286
                    // here we need to make sure that we aren't sending a value to 
                    // the js engine that might be modifed by the actions of the js engine
                    // for example, calling put() mgiht cause the original data to change 
                    // because we defrag the data that we looked at. We are handling this by
                    // ensuring that we have our own, safe, copy.
                    var cloned = origin.Clone(context);
                    _disposables.Add(cloned);
                    return cloned;
                }

                if (o is Document d)
                    return new BlittableObjectInstance(engine, Clone(d.Data), d.Id, d.LastModified);
                if (o is DocumentConflict dc)
                    return new BlittableObjectInstance(engine, Clone(dc.Doc), dc.Id, dc.LastModified);
                if (o is BlittableJsonReaderObject json)
                    return new BlittableObjectInstance(engine, json, null, null);
                // Removing this for now to see what breaks
                //if (o is BlittableJsonReaderArray array)
                //    return BlittableObjectInstance.CreateArrayInstanceBasedOnBlittableArray(engine, array);
                if (o == null)
                    return Null.Instance;
                if (o is long)
                    return o;
                if (o is List<object> l)
                {
                    for (int i = 0; i < l.Count; i++)
                    {
                        l[i] = TranslateToJs(ScriptEngine, context, l[i]);
                    }
                    return l;
                }
                // for admin
                if (o is RavenServer || o is DocumentDatabase)
                {
                    AssertAdminScriptInstance();
                    return o;
                }
                if (o is ObjectInstance)
                {
                    return o;
                }
#if DEBUG
                Debug.Assert(ExpectedTypes.Contains(o.GetType()));
#endif
                return o;
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

            internal static Action GetUselessOnStateLoopIterationInstanceForCodeGenerationOnly()
            {
                return new SingleRun().OnStateLoopIteration;
            }

            public void SetGlobalFunction(string functionName, Delegate functionInstance)
            {
                ScriptEngine.SetValue(functionName,functionInstance);
            }

            
            public object Translate(ScriptRunnerResult result, JsonOperationContext context, BlittableJsonDocumentBuilder.UsageMode usageMode = BlittableJsonDocumentBuilder.UsageMode.None)
            {
                if (result.Value is ObjectInstance)
                    return result.Translate(_context, usageMode);

                return result.Value;
            }
        }

        public ScriptRunner(DocumentDatabase db, bool enableClr)
        {
            _db = db;
            _enableClr = enableClr;
        }

        private readonly ConcurrentQueue<SingleRun> _cache = new ConcurrentQueue<SingleRun>();

        public long Runs;

        public ReturnRun GetRunner(out SingleRun run)
        {
            if (_cache.TryDequeue(out run) == false)
            {
                run = new SingleRun(_db, this, ScriptsSource);
            }
            Interlocked.Increment(ref Runs);
            return new ReturnRun(this, run);
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

        public void TryCompileScript(string script)
        {
            try
            {
                var engine = new Jint.Engine(options =>
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
    }
}
