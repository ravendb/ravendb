using Lucene.Net.Analysis;
using Raven.Client.Documents.Queries;

namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public class LuceneASTQueryConfiguration
    {
        public Analyzer Analyzer { get; set; }
        public FieldName FieldName { get; set; }
        public QueryOperator DefaultOperator { get; set; }
    }
}