using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Raven.Server.Utils.Stats
{
    public interface IStatsScope
    {
        TimeSpan Duration { get; }
    }

    public abstract class StatsScope<T, TStatsScope> : IStatsScope, IDisposable 
        where TStatsScope : StatsScope<T, TStatsScope>
    {
        private readonly Stopwatch _sw;
        private readonly T _stats;
        private Dictionary<string, TStatsScope> _scopes;
        protected List<KeyValuePair<string, TStatsScope>> Scopes;

        protected StatsScope(T stats, bool start = true)
        {
            _stats = stats;
            _sw = new Stopwatch();

            if (start)
                Start();
        }

        public TimeSpan Duration => _sw.Elapsed;

        public T CurrentStats => _stats;

        public TStatsScope Start()
        {
            _sw.Start();
            return this as TStatsScope;
        }

        protected abstract TStatsScope OpenNewScope(T stats, bool start);

        public TStatsScope For(string name, bool start = true)
        {
            if (_scopes == null)
                _scopes = new Dictionary<string, TStatsScope>(StringComparer.OrdinalIgnoreCase);

            if (Scopes == null)
                Scopes = new List<KeyValuePair<string, TStatsScope>>();

            if (_scopes.TryGetValue(name, out TStatsScope scope) == false)
            {
                var kvp = new KeyValuePair<string, TStatsScope>(name, OpenNewScope(_stats, start));
                Scopes.Add(kvp);
                return _scopes[name] = kvp.Value;
            }

            if (start)
                scope.Start();

            return scope;
        }

        public void Dispose()
        {
            _sw?.Stop();
        }
    }
}
