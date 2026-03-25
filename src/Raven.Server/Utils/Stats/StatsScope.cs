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
        private const long NotStarted = 0;

        // A stopwatch replacement that takes only 2 fields.
        private long _start;
        private TimeSpan _elapsed;

        private readonly T _stats;
        private Dictionary<string, TStatsScope> _scopes;
        protected List<KeyValuePair<string, TStatsScope>> Scopes;

        protected StatsScope(T stats, bool start = true)
        {
            _stats = stats;

            if (start)
                Start();
        }

        /// <summary>
        /// Gets the duration or sets it in a forceful way.
        /// </summary>
        public TimeSpan Duration
        {
            get => _elapsed + (_start != NotStarted ? Stopwatch.GetElapsedTime(_start) : TimeSpan.Zero);
            set
            {
                _elapsed = value;
                // Clear if it was started before
                _start = NotStarted;
            }
        }

        public T CurrentStats => _stats;

        public TStatsScope Start()
        {
            _start = Stopwatch.GetTimestamp();
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
            if (_start == NotStarted)
                return;

            // Add elapsed
            _elapsed += Stopwatch.GetElapsedTime(_start);

            // Pause the timer
            _start = NotStarted;
        }
    }
}
