using System;
using Corax;

namespace Raven.Server.Documents.Indexes.Persistence.Corax
{
    public class CoraxIndexingExtensions
    {
        public static Analyzer CreateAnalyzerInstance(string fieldName, Type analyzerType)
        {
            throw new NotImplementedException("Custom analyzers are not allowed in Corax.");
        }
    }
}
