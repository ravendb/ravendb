using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using Jurassic;
using Jurassic.Compiler;
using Jurassic.Library;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunnerObject
    {
        private readonly ObjectInstance _instance;

        public ScriptRunnerObject(ObjectInstance instance)
        {
            _instance = instance;
        }
    }

    public class ScriptRunnerCache
    {
        private readonly DocumentDatabase _database;

        private readonly ConcurrentDictionary<string, Lazy<ScriptRunner>> _cache =
            new ConcurrentDictionary<string, Lazy<ScriptRunner>>();
        private int _numberOfCachedScripts;
        private SpinLock _cleaning = new SpinLock();

        public ScriptRunnerCache(DocumentDatabase database)
        {
            _database = database;
        }

        public ScriptRunner GetScriptRunner(string script)
        {
            Lazy<ScriptRunner> lazy;
            if (_cache.TryGetValue(script, out lazy))
                return lazy.Value;

            return GetScriptRunnerUnlikely(script);
        }

        private ScriptRunner GetScriptRunnerUnlikely(string script)
        {
            var value = new Lazy<ScriptRunner>(() =>
            {
                var runner = new ScriptRunner(_database);
                runner.AddScript(script);
                return runner;
            });
            var lazy = _cache.GetOrAdd(script, value);
            if (value != lazy)
                return lazy.Value;

            // we were the one who added it, need to check that we are there
            var count = Interlocked.Increment(ref _numberOfCachedScripts);
            if (count > 2048)// TODO: Maxim make this configurable
            {
                bool taken = false;
                try
                {
                    _cleaning.TryEnter(ref taken);
                    if (taken)
                    {
                        // TODO: Alert if we are doing this cleanup too often 
                        var numRemaining = CleanTheCache();
                        Interlocked.Add(ref _numberOfCachedScripts, -(count - numRemaining));
                    }
                }
                finally
                {
                    if(taken)
                        _cleaning.Exit();
                }
                    
            }
            return lazy.Value;
        }

        private int CleanTheCache()
        {
            foreach (var pair in _cache
                .OrderBy(x => x.Value.Value.Runs)
                .Take(512)
            )
            {
                _cache.TryRemove(pair.Key, out _);
            }
            int count = 0;
            foreach (var pair in _cache)
            {
                count++;
                var valueRuns = pair.Value.Value.Runs / 2;
                Interlocked.Add(ref pair.Value.Value.Runs, -valueRuns);
            }

            return count;
        }
    }

    public class ScriptRunner
    {
        private readonly DocumentDatabase _db;
        private readonly List<CompiledScript> _scripts = new List<CompiledScript>();

        public void AddScript(string script)
        {
            var compiledScript = CompiledScript.Compile(new StringScriptSource(script),
                new CompilerOptions
                {
                    EmitOnLoopIteration = new SingleRun(null).OnStateLoopIteration
                });

            _scripts.Add(compiledScript);
        }

        public void AddScript(CompiledScript compiledScript)
        {
            _scripts.Add(compiledScript);
        }

        public class SingleRun
        {
            public SingleRun(DocumentDatabase database)
            {
                _database = database;
                ScriptEngine = new ScriptEngine
                {
                    RecursionDepthLimit = 64,
                    OnLoopIterationCall = OnStateLoopIteration
                };
                ScriptEngine.SetGlobalFunction("load", (Func<string, object>)LoadDocument);
                ScriptEngine.SetGlobalFunction("id", (Func<object, string>)GetDocumentId);
            }

            private string GetDocumentId(object arg)
            {
                if (arg is BlittableObjectInstance doc)
                    return doc.Document?.Id;
                return null;
            }

            private object LoadDocument(string id)
            {
                var document = _database.DocumentsStorage.Get(_context, id);
                if (document == null)
                    return Null.Value;
                return new BlittableObjectInstance(ScriptEngine, document.Data, document);
            }

            private readonly DocumentDatabase _database;
            private DocumentsOperationContext _context;

            public void Initialize(DocumentsOperationContext context)
            {
                _context = context;
            }

            public int MaxSteps;
            public int CurrentSteps;
            public readonly ScriptEngine ScriptEngine;

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

            public object Run(string method, object[] args)
            {
                CurrentSteps = 0;
                MaxSteps = 1000;
                for (int i = 0; i < args.Length; i++)
                {
                    args[i] = TranslateToJurrasic(ScriptEngine, args[i]);
                }
                var result = ScriptEngine.CallGlobalFunction(method, args);
                return TranslateFromJurrasic(result);
            }

            private object TranslateFromJurrasic(object result)
            {
                if (result is ArrayInstance)
                    ThrowInvalidArrayResult();
                if (result is ObjectInstance obj)
                    return new ScriptRunnerObject(obj);
                return result;
            }

            private static void ThrowInvalidArrayResult() =>
                throw new InvalidOperationException("Script cannot return an array.");


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

            private object TranslateToJurrasic(ScriptEngine engine, object o)
            {
                if (o is Document d)
                    return new BlittableObjectInstance(engine, d.Data, d);
                if (o is BlittableJsonReaderObject json)
                    return new BlittableObjectInstance(engine, json, null);
                if (o is BlittableJsonReaderArray array)
                    return BlittableObjectInstance.CreateArrayInstanceBasedOnBlittableArray(engine, array);
                if (o == null)
                    return Null.Value;
#if DEBUG
                Debug.Assert(ExpectedTypes.Contains(o.GetType()));
#endif
                return o;
            }
        }

        public ScriptRunner(DocumentDatabase db)
        {
            _db = db;
        }

        private readonly ConcurrentQueue<SingleRun> _cache = new ConcurrentQueue<SingleRun>();

        public long Runs;

        public ReturnRun GetRunner(DocumentsOperationContext context, out SingleRun run)
        {
            if (_cache.TryDequeue(out run) == false)
            {
                run = new SingleRun(_db);
            }
            run.Initialize(context);
            Interlocked.Increment(ref Runs);
            return new ReturnRun(this, run);
        }

        public struct ReturnRun : IDisposable
        {
            private readonly ScriptRunner _parent;
            private readonly SingleRun _run;

            public ReturnRun(ScriptRunner parent, SingleRun run)
            {
                _parent = parent;
                _run = run;
            }

            public void Dispose()
            {
                _parent._cache.Enqueue(_run);
            }
        }



    }

}
