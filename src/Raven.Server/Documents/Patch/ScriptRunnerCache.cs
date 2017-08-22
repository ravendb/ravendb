using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Threading;

namespace Raven.Server.Documents.Patch
{
    public class ScriptRunnerCache
    {
        private readonly DocumentDatabase _database;

        private readonly ConcurrentDictionary<Key, Lazy<ScriptRunner>> _cache =
            new ConcurrentDictionary<Key, Lazy<ScriptRunner>>();

        private int _numberOfCachedScripts;
        private SpinLock _cleaning = new SpinLock();
        public bool EnableClr;

        public ScriptRunnerCache(DocumentDatabase database)
        {
            _database = database;
        }

        public abstract class Key
        {
            public string ScriptKey;

            public abstract void GenerateScript(ScriptRunner runner);

            public abstract override bool Equals(object obj);

            public abstract override int GetHashCode();
        }


        public ScriptRunner.ReturnRun GetScriptRunner(Key key, out ScriptRunner.SingleRun patchRun)
        {
            if (key == null)
            {
                patchRun = null;
                return new ScriptRunner.ReturnRun();
            }
            return GetScriptRunner(key).GetRunner(out patchRun);
        }


        private ScriptRunner GetScriptRunner(Key script)
        {
            Lazy<ScriptRunner> lazy;
            if (_cache.TryGetValue(script, out lazy))
                return lazy.Value;

            return GetScriptRunnerUnlikely(script);
        }

        private ScriptRunner GetScriptRunnerUnlikely(Key script)
        {
            var value = new Lazy<ScriptRunner>(() =>
            {
                var runner = new ScriptRunner(_database, EnableClr);
                script.GenerateScript(runner);
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
                    if (taken)
                        _cleaning.Exit();
                }

            }
            return lazy.Value;
        }

        private int CleanTheCache()
        {
            foreach (var pair in Enumerable.OrderBy(_cache, x => x.Value.Value.Runs)
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
}
