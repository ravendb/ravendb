extern alias NGC;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using JetBrains.Annotations;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Raven.Server.Documents.Queries.AST;
using Raven.Server.Documents.Queries.Results.TimeSeries;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using JavaScriptException = Jint.Runtime.JavaScriptException;

namespace Raven.Server.Documents.Patch
{
    public abstract class ScriptRunner<T>
        where T : struct, IJsHandle<T>
    {
        protected readonly ConcurrentQueue<Holder<T>> _cache = new ConcurrentQueue<Holder<T>>();

        internal readonly ScriptRunnerCache _parent;
        internal readonly bool _enableClr;
        protected readonly DateTime _creationTime;
        public readonly List<string> ScriptsSource = new List<string>();

        public int NumberOfCachedScripts => _cache.Count(x =>
            x.Value != null ||
            x.WeakValue?.TryGetTarget(out _) == true);

        internal readonly Dictionary<string, DeclaredFunction> TimeSeriesDeclaration = new Dictionary<string, DeclaredFunction>();

        public long Runs;
        protected DateTime _lastRun;
        private readonly JavaScriptEngineType _jsEngine;

        public string ScriptType { get; internal set; }

        protected ScriptRunner([NotNull] ScriptRunnerCache parent, bool enableClr)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
            _enableClr = enableClr;
            _creationTime = DateTime.UtcNow;
            _jsEngine = _parent.Configuration.JavaScript.EngineType;
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

        public ReturnRun GetRunner(out ISingleRun run, bool executeScriptsSource = true)
        {
            _lastRun = DateTime.UtcNow;
            Interlocked.Increment(ref Runs);

            var holder = GetSingleRunHolder(executeScriptsSource);
            run = holder.Value;
            return new ReturnRun(run, holder);
        }

        public ReturnRun GetRunner(out SingleRun<T> run, bool executeScriptsSource = true) 
        {
            _lastRun = DateTime.UtcNow;
            Interlocked.Increment(ref Runs);

            var holder = GetSingleRunHolder(executeScriptsSource);
            run = holder.Value;
            return new ReturnRun(run, holder);
        }

        //public ReturnRun GetRunner(out SingleRunJint run, bool executeScriptsSource = true)
        //{
        //    _lastRun = DateTime.UtcNow;
        //    Interlocked.Increment(ref Runs);

        //    var holder = GetSingleRunHolder(executeScriptsSource);
        //    run = holder.Value as SingleRunJint;
        //    return new ReturnRun(run, holder);
        //}

        //public ReturnRun GetRunner(out SingleRunV8 run, bool executeScriptsSource = true)
        //{
        //    _lastRun = DateTime.UtcNow;
        //    Interlocked.Increment(ref Runs);

        //    var holder = GetSingleRunHolder(executeScriptsSource);
        //    run = holder.Value as SingleRunV8;
        //    return new ReturnRun(run, holder);
        //}

        //public ReturnRun GetRunner<T>(out SingleRun<T> run, bool executeScriptsSource) where T : struct, IJsHandle<T>
        //{
        //    _lastRun = DateTime.UtcNow;
        //    Interlocked.Increment(ref Runs);

        //    Holder<T> holder = GetSingleRunHolder(executeScriptsSource);
        //    run = holder.Value;
        //    return new ReturnRun(run, holder);
        //}
        public abstract Holder<T> GetSingleRunHolder(bool executeScriptsSource = true);
        public abstract ReturnRun GetRunnerTyped(out SingleRun<T> run, bool executeScriptsSource = true);

        public void ReturnRunner(Holder<T> holder)
        {
            if (holder == null)
                return;

            if (holder.Generation != _parent.Generation)
                return;

            _cache.Enqueue(holder);
        }

        public static void TryCompileScript(string script)
        {
            //var jsEngineType = JsOptions.EngineType;
            //IJsEngineHandle tryScriptEngineHandle = jsEngineType switch
            //{
            //    JavaScriptEngineType.Jint => new PatchJint.JintEngineEx(),
            //    JavaScriptEngineType.V8 => GetSingleRunHolder().Value.ScriptEngineHandle,
            //    _ => throw new NotSupportedException($"Not supported JS engine type '{JsOptions}'.")
            //};
         //   tryScriptEngineHandle.TryCompileScript(script);
        }

        public static unsafe DateTime GetDateArg(T arg, string signature, string argName)
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

        private static DateTime GetTimeSeriesDateArg(T arg, string signature, string argName)
        {
            if (arg.IsDate)
                return arg.AsDate;

            if (arg.IsStringEx == false)
                throw new ArgumentException($"{signature} : {argName} must be of type 'DateInstance' or a DateTime string. {GetTypes(arg)}");

            return TimeSeriesRetriever.ParseDateTime(arg.AsString);
        }
        
        private static string GetTypes(T value) => $"JintType({value.ValueType}) .NETType({value.GetType().Name})";


        public bool RunIdleOperations()
        {
            while (_cache.TryDequeue(out var holder))
            {
                var val = holder.Value;
                if (val != null)
                {
                    // move the cache to weak reference value
                    holder.WeakValue = new WeakReference<SingleRun<T>>(val);
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

    public class ScriptRunnerV8 : ScriptRunner<JsHandleV8>
    {
        public ScriptRunnerV8([NotNull] ScriptRunnerCache parent, bool enableClr) : base(parent, enableClr)
        {
        }

        public override Holder<JsHandleV8> GetSingleRunHolder(bool executeScriptsSource = true)
        {
            if (_cache.TryDequeue(out Holder<JsHandleV8> holder) == false)
            {
                holder = new Holder<JsHandleV8>(_parent.Generation)
                {
                    Parent = this
                };
            }

            if (holder.Value == null)
            {
                if (holder.WeakValue != null &&
                    holder.WeakValue.TryGetTarget(out SingleRun<JsHandleV8> run))
                {
                    holder.Value = run;
                    holder.WeakValue = null;
                }
                else
                {
                    holder.Value = new SingleRunV8(_parent.Database, _parent.Configuration, this, ScriptsSource, executeScriptsSource);
                }
            }

            return holder;
        }

        public override ReturnRun GetRunnerTyped(out SingleRun<JsHandleV8> run, bool executeScriptsSource = true)
        {
            _lastRun = DateTime.UtcNow;
            Interlocked.Increment(ref Runs);

            var holder = GetSingleRunHolder(executeScriptsSource);
            run = holder.Value;
            return new ReturnRun(run, holder);
        }
    }

    public class ScriptRunnerJint : ScriptRunner<JsHandleJint>
    {
        public ScriptRunnerJint([NotNull] ScriptRunnerCache parent, bool enableClr) : base(parent, enableClr)
        {
        }

        public override Holder<JsHandleJint> GetSingleRunHolder(bool executeScriptsSource = true)
        {
            if (_cache.TryDequeue(out Holder<JsHandleJint> holder) == false)
            {
                holder = new Holder<JsHandleJint>(_parent.Generation)
                {
                    Parent = this
                };
            }

            if (holder.Value == null)
            {
                if (holder.WeakValue != null &&
                    holder.WeakValue.TryGetTarget(out SingleRun<JsHandleJint> run))
                {
                    holder.Value = run;
                    holder.WeakValue = null;
                }
                else
                {
                    holder.Value = new SingleRunJint(_parent.Database, _parent.Configuration, this, ScriptsSource, executeScriptsSource);
                }
            }

            return holder;
        }

        public override ReturnRun GetRunnerTyped(out SingleRun<JsHandleJint> run, bool executeScriptsSource = true)
        {
            throw new NotImplementedException();
        }
    }

    public struct ReturnRun : IDisposable
    {
        private ISingleRun _run;
        private IHolder _holder;

        public ReturnRun(ISingleRun run, IHolder holder)
        {
            _run = run;
            _holder = holder;
        }

        public void Dispose()
        {
            if (_run == null)
                return;
            _run.CleanStuff();
            _holder.ReturnHolder();
            _holder = null;
            _run = null;
        }
    }

    public interface IHolder
    {
        void ReturnHolder();
    }

    public class Holder<T> : IHolder
        where T : struct, IJsHandle<T>
    {
        public Holder(long generation)
        {
            Generation = generation;
        }

        public readonly long Generation;
        public ScriptRunner<T> Parent;
        public SingleRun<T> Value;
        public WeakReference<SingleRun<T>> WeakValue;
        public void ReturnHolder()
        {
            Parent.ReturnRunner(this);
        }
    }

}
