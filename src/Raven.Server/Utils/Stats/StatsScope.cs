using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Raven.Server.Utils.Stats
{
    public abstract class StatsScope<T, TStatsScope> : IDisposable where TStatsScope : StatsScope<T, TStatsScope>
    {
        private readonly Stopwatch _sw;
        private readonly T _stats;
        protected Dictionary<string, TStatsScope> Scopes;

        protected StatsScope(T stats, bool start = true)
        {
            _stats = stats;
            _sw = new Stopwatch();

            if (start)
                Start();
        }

        public TimeSpan Duration => _sw.Elapsed;

        public TStatsScope Start()
        {
            _sw.Start();
            return this as TStatsScope;
        }

        protected abstract TStatsScope OpenNewScope(T stats, bool start);

        public TStatsScope For(string name, bool start = true)
        {
            if (Scopes == null)
                Scopes = new Dictionary<string, TStatsScope>(StringComparer.OrdinalIgnoreCase);

            TStatsScope scope;
            if (Scopes.TryGetValue(name, out scope) == false)
                return Scopes[name] = OpenNewScope(_stats, start);

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