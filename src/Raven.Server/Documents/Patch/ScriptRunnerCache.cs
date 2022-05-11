using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using JetBrains.Annotations;
using Raven.Client.ServerWide.JavaScript;
using Raven.Server.Config;
using Raven.Server.Documents.Patch.Jint;
using Raven.Server.Documents.Patch.V8;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunnerCache : ILowMemoryHandler
    {
        internal static string PolyfillJs;

        private long _generation;

        private RavenConfiguration _configuration;

        private readonly ConcurrentDictionary<Key, Lazy<ScriptRunnerV8>> _scriptRunnerCacheV8 = new ConcurrentDictionary<Key, Lazy<ScriptRunnerV8>>();
        private readonly ConcurrentDictionary<Key, Lazy<ScriptRunnerJint>> _scriptRunnerCacheJint = new ConcurrentDictionary<Key, Lazy<ScriptRunnerJint>>();

        public bool EnableClr;

        public readonly DocumentDatabase Database;
        private readonly JavaScriptEngineType _engineType;

        public RavenConfiguration Configuration => _configuration;

        public long Generation => _generation;

        static ScriptRunnerCache()
        {
            using (Stream stream = typeof(ScriptRunnerCache).Assembly.GetManifestResourceStream("Raven.Server.Documents.Patch.Polyfill.js"))
            {
                using (var reader = new StreamReader(stream))
                {
                    PolyfillJs = reader.ReadToEnd();
                }
            }
        }

        public ScriptRunnerCache(DocumentDatabase database, [NotNull] RavenConfiguration configuration)
        {
            Database = database;
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));
            _engineType = _configuration.JavaScript.EngineType;

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        public int NumberOfCachedScripts
        {
            get
            {
                var x1 = _scriptRunnerCacheV8.Values
                    .Select(x => x.IsValueCreated ? x.Value : null)
                    .Where(x => x != null)
                    .Sum(x => x.NumberOfCachedScripts);

                var x2 = _scriptRunnerCacheJint.Values
                    .Select(x => x.IsValueCreated ? x.Value : null)
                    .Where(x => x != null)
                    .Sum(x => x.NumberOfCachedScripts);

                return x1 + x2;
            }
        }

        public IEnumerable<DynamicJsonValue> GetDebugInfo(bool detailed = false)
        {
            foreach (var item in _scriptRunnerCacheV8)
            {
                yield return item.Value.Value.GetDebugInfo(detailed);
            }

            foreach (var item in _scriptRunnerCacheJint)
            {
                yield return item.Value.Value.GetDebugInfo(detailed);
            }
        }

        public abstract class Key
        {
            public abstract void GenerateScript<T>(ScriptRunner<T> runner) where T : struct, IJsHandle<T>;

            public abstract override bool Equals(object obj);

            public abstract override int GetHashCode();
        }
        
        public ReturnRun GetScriptRunner(Key key, bool readOnly, out ISingleRun patchRun, bool executeScriptsSource = true)
        {
            if (key == null)
            {
                patchRun = null;
                return new ReturnRun();
            }

            ReturnRun returnRun;
            switch (_engineType)
            {
                case JavaScriptEngineType.Jint:
                    returnRun = TryGetJintScriptRunnerFromCache(key).GetRunner(out patchRun, executeScriptsSource);
                    break;
                case JavaScriptEngineType.V8:
                    returnRun = TryGetV8ScriptRunnerFromCache(key).GetRunner(out patchRun, executeScriptsSource);
                    break;
                default:
                    throw new ArgumentOutOfRangeException();
            }
            patchRun.ReadOnly = false;
           // patchRun.SetReadOnly(readOnly);
            return returnRun;
        }
        public ReturnRun GetScriptRunnerJint(Key key, bool readOnly, out SingleRunJint patchRun, bool executeScriptsSource = true)
        {
            if (key == null)
            {
                patchRun = null;
                return new ReturnRun();
            }

            ReturnRun returnRun = TryGetJintScriptRunnerFromCache(key).GetRunner(out patchRun, executeScriptsSource);
            patchRun.ReadOnly = false;
            // patchRun.SetReadOnly(readOnly);
            return returnRun;
        }
        public ReturnRun GetScriptRunnerJint(Key key, bool readOnly, out SingleRun<JsHandleJint> patchRun, bool executeScriptsSource = true)
        {
            if (key == null)
            {
                patchRun = null;
                return new ReturnRun();
            }

            ReturnRun returnRun = TryGetJintScriptRunnerFromCache(key).GetRunner(out patchRun, executeScriptsSource);
            patchRun.ReadOnly = false;
            // patchRun.SetReadOnly(readOnly);
            return returnRun;
        }
        public ReturnRun GetScriptRunnerV8(Key key, bool readOnly, out SingleRunV8 patchRun, bool executeScriptsSource = true)
        {
            if (key == null)
            {
                patchRun = null;
                return new ReturnRun();
            }

            ReturnRun returnRun = TryGetV8ScriptRunnerFromCache(key).GetRunner(out patchRun, executeScriptsSource);
            patchRun.ReadOnly = false;
            // patchRun.SetReadOnly(readOnly);
            return returnRun;
        }
        public ReturnRun GetScriptRunnerV8(Key key, bool readOnly, out SingleRun<JsHandleV8> patchRun, bool executeScriptsSource = true)
        {
            if (key == null)
            {
                patchRun = null;
                return new ReturnRun();
            }

            ReturnRun returnRun = TryGetV8ScriptRunnerFromCache(key).GetRunner(out patchRun, executeScriptsSource);
            patchRun.ReadOnly = false;
            // patchRun.SetReadOnly(readOnly);
            return returnRun;
        }

        private ScriptRunnerJint TryGetJintScriptRunnerFromCache(Key script)
        {
            if (_scriptRunnerCacheJint.TryGetValue(script, out var lazy))
                return lazy.Value;

            var value = new Lazy<ScriptRunnerJint>(() =>
            {
                var runner = new ScriptRunnerJint(this, EnableClr);
                script.GenerateScript(runner);
                runner.ScriptType = script.GetType().Name;
                return runner;
            });
            return _scriptRunnerCacheJint.GetOrAdd(script, value).Value;
        }

        private ScriptRunnerV8 TryGetV8ScriptRunnerFromCache(Key script)
        {
            if (_scriptRunnerCacheV8.TryGetValue(script, out var lazy))                
                return lazy.Value;

            var value = new Lazy<ScriptRunnerV8>(() =>
            {
                var runner = new ScriptRunnerV8(this, EnableClr);
                script.GenerateScript(runner);
                runner.ScriptType = script.GetType().Name;
                return runner;
            });
            return _scriptRunnerCacheV8.GetOrAdd(script, value).Value;
        }

        public void UpdateConfiguration(RavenConfiguration configuration)
        {
            _configuration = configuration;
            Interlocked.Increment(ref _generation);
        }

        public void LowMemory(LowMemorySeverity lowMemorySeverity)
        {
            _scriptRunnerCacheV8.Clear();
        }

        public void LowMemoryOver()
        {
        }

        public void RunIdleOperations()
        {
            foreach (var (key, lazyRunner) in _scriptRunnerCacheV8)
            {
                if (lazyRunner.IsValueCreated == false)
                    continue;
                if (lazyRunner.Value.RunIdleOperations() == false)
                    _scriptRunnerCacheV8.TryRemove(key, out _);
            }
        }
    }
}
