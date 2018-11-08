using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using JetBrains.Annotations;
using Raven.Client.Extensions;
using Raven.Server.Config;
using Sparrow.Json.Parsing;
using Sparrow.LowMemory;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunnerCache : ILowMemoryHandler
    {
        private readonly DocumentDatabase _database;
        private readonly RavenConfiguration _configuration;

        private readonly ConcurrentDictionary<Key, Lazy<ScriptRunner>> _cache =
            new ConcurrentDictionary<Key, Lazy<ScriptRunner>>();

        private int _numberOfCachedScripts;
        private SpinLock _cleaning = new SpinLock();
        public bool EnableClr;
        internal static string PolyfillJs;

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
            _database = database;
            _configuration = configuration ?? throw new ArgumentNullException(nameof(configuration));

            LowMemoryNotification.Instance.RegisterLowMemoryHandler(this);
        }

        public IEnumerable<DynamicJsonValue> GetDebugInfo(bool detailed = false)
        {
            foreach (var item in _cache)
            {
                yield return item.Value.Value.GetDebugInfo(detailed);
            }
        }

        public abstract class Key
        {
            public abstract void GenerateScript(ScriptRunner runner);

            public abstract override bool Equals(object obj);

            public abstract override int GetHashCode();
        }


        public ScriptRunner.ReturnRun GetScriptRunner(Key key, bool readOnly, out ScriptRunner.SingleRun patchRun)
        {
            if (key == null)
            {
                patchRun = null;
                return new ScriptRunner.ReturnRun();
            }
            var scriptRunner = GetScriptRunner(key);
            var returnRun = scriptRunner.GetRunner(out patchRun);
            patchRun.ReadOnly = readOnly;
            return returnRun;
        }


        private ScriptRunner GetScriptRunner(Key script)
        {
            if (_cache.TryGetValue(script, out var lazy))                
                return lazy.Value;

            return GetScriptRunnerUnlikely(script);
        }

        private ScriptRunner GetScriptRunnerUnlikely(Key script)
        {
            var value = new Lazy<ScriptRunner>(() =>
            {
                var runner = new ScriptRunner(_database, _configuration, EnableClr);
                script.GenerateScript(runner);
                runner.ScriptType = script.GetType().Name;
                return runner;
            });
            var lazy = _cache.GetOrAdd(script, value);
            if (value != lazy)
                return lazy.Value;

            // we were the one who added it, need to check that we are there
            var count = Interlocked.Increment(ref _numberOfCachedScripts);
            if (count > _configuration.Patching.MaxNumberOfCachedScripts)
            {
                bool taken = false;
                try
                {
                    _cleaning.TryEnter(ref taken);
                    if (taken)
                    {
                        var numRemaining = CleanTheCache();
                        Interlocked.Add(ref _numberOfCachedScripts, -(count - numRemaining));
                    }
                }
                finally
                {
                    if (taken)
                        _cleaning.Exit();
                }

            }
            return lazy.Value;
        }


        private int CleanTheCache()
        {
            
            foreach (var pair in _cache.ForceEnumerateInThreadSafeManner().OrderBy(x => x.Value.Value.Runs)
                .Take(_configuration.Patching.MaxNumberOfCachedScripts / 4)
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

        public void LowMemory()
        {
            _cache.Clear();
        }

        public void LowMemoryOver()
        {
        }
    }
}
