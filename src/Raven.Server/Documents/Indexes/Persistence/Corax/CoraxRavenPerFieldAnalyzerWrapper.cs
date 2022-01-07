using System;
using System.Collections.Generic;
using Corax;
using Raven.Server.Utils;
using Sparrow.Server;
using Sparrow.Threading;
using Voron;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public sealed class CoraxRavenPerFieldAnalyzerWrapper : IDisposable
    {
        private readonly IndexFieldsMapping _analyzers;
        private readonly ByteStringContext _context;
        public IndexFieldsMapping Analyzers => _analyzers;
        
        public CoraxRavenPerFieldAnalyzerWrapper(Analyzer defaultAnalyzer, int fieldCount)
        {
            _context = new ByteStringContext(SharedMultipleUseFlag.None);

            _analyzers = new IndexFieldsMapping(_context);
            for(int i = 0; i < fieldCount; ++i)
            {
                Slice.From(_context, $"Field{i}", out var fieldName);
                _analyzers.AddBinding(i, fieldName, defaultAnalyzer);
            }
        }

        public CoraxRavenPerFieldAnalyzerWrapper(Analyzer defaultAnalyzer, Func<string, Analyzer> defaultSearchAnalyzerFactory,
            Func<string, Analyzer> defaultExactAnalyzerFactory, int fieldCount)
            : this(defaultAnalyzer, fieldCount)
        {
        }

        public void Dispose()
        {
            _context.Dispose();
        }

        public void AddAnalyzer(int fieldId, string fieldName, Analyzer analyzer)
        {
            Slice.From(_context, fieldName, out var fieldSlice);
            _analyzers.AddBinding(fieldId, fieldSlice, analyzer);
        }
    }
}
