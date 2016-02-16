using System;
using System.Collections.Concurrent;
using System.Collections.Generic;

namespace metrics
{
    /// <summary>
    /// A manager class for health checks
    /// </summary>
    public class HealthChecks
    {
        private static readonly ConcurrentDictionary<string, HealthCheck> _checks = new ConcurrentDictionary<string, HealthCheck>();

        private HealthChecks() { }

        /// <summary>
        /// Registers an application <see cref="HealthCheck" /> with a given name
        /// </summary>
        /// <param name="name">The named health check instance</param>
        /// <param name="check">The <see cref="HealthCheck" /> function</param>
        public static void Register(string name, Func<HealthCheck.Result> check)
        {
            var healthCheck = new HealthCheck(name, check);
            if(!_checks.ContainsKey(healthCheck.Name))
            {
                _checks.TryAdd(healthCheck.Name, healthCheck);
            }
        }

        /// <summary>
        /// Returns <code>true</code>  <see cref="HealthCheck"/>s have been registered, <code>false</code> otherwise
        /// </summary>
        public static bool HasHealthChecks { get { return _checks.IsEmpty; }}
        
        /// <summary>
        /// Runs the registered health checks and returns a map of the results.
        /// </summary>
        public static IDictionary<string, HealthCheck.Result> RunHealthChecks()
        {
            var results = new SortedDictionary<string, HealthCheck.Result>();
            foreach (var entry in _checks)
            {
                var result = entry.Value.Execute();
                results.Add(entry.Key, result);
            }
            return results;
        }
    }
}


