using System;
using Lucene.Net.Analysis;
using Raven.Server.Documents.Indexes.Persistence.Lucene;

namespace Raven.Server.Documents.Indexes.Analysis
{
    public class AnalyzerFactory
    {
        public readonly Type Type;

        public AnalyzerFactory(Type analyzerType)
        {
            Type = analyzerType ?? throw new ArgumentNullException(nameof(analyzerType));
        }

        public virtual Analyzer CreateInstance(string fieldName)
        {
            return IndexingExtensions.CreateAnalyzerInstance(fieldName, Type);
        }
    }

    public class FaultyAnalyzerFactory : AnalyzerFactory
    {
        private readonly string _name;
        private readonly Exception _e;

        public FaultyAnalyzerFactory(string name, Exception e)
            : base(typeof(Analyzer))
        {
            _name = name;
            _e = e;
        }

        public override Analyzer CreateInstance(string fieldName)
        {
            throw new NotSupportedException($"Analyzer {_name} is an implementation of a faulty analyzer", _e);
        }
    }
}
