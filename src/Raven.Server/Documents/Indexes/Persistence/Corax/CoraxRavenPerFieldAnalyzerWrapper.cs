using System;
using System.Collections.Generic;
using Corax;
using Raven.Server.Utils;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public sealed class CoraxRavenPerFieldAnalyzerWrapper : IDisposable
    {
        private readonly Dictionary<int, Analyzer> _analyzers;
        public Dictionary<int, Analyzer> Analyzers => _analyzers;
        
        public CoraxRavenPerFieldAnalyzerWrapper(Analyzer defaultAnalyzer, int fieldCount)
        {
            _analyzers = new();
            for(int i = 0; i < fieldCount; ++i)
            {
                _analyzers.Add(i, defaultAnalyzer);
            }
        }

        public CoraxRavenPerFieldAnalyzerWrapper(Analyzer defaultAnalyzer, Func<string, Analyzer> defaultSearchAnalyzerFactory,
            Func<string, Analyzer> defaultExactAnalyzerFactory, int fieldCount)
            : this(defaultAnalyzer, fieldCount)
        {
        }
        
        public void AddAnalyzer(int fieldId, Analyzer analyzer)
        {
            if (_analyzers.TryAdd(fieldId, analyzer) == false)
            {
                _analyzers[fieldId] = analyzer;
            }
        }

        public void Dispose()
        {
            var exceptionAggregator = new ExceptionAggregator($"Could not dispose {nameof(CoraxRavenPerFieldAnalyzerWrapper)}.");
            
            exceptionAggregator.Execute(() =>
            {
                foreach(var disposableItem in _analyzers.Values)
                    disposableItem?.Dispose();
            });
            
            exceptionAggregator.ThrowIfNeeded();
        }
    }
}
