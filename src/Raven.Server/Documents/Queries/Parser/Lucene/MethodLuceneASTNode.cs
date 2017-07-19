using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Server.Documents.Queries.LuceneIntegration;

namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public class MethodLuceneASTNode : LuceneASTNodeBase
    {
        public MethodLuceneASTNode(string rawMethodStr, List<TermLuceneASTNode> matches)
        {
            var fieldStartPos = rawMethodStr.IndexOf('<');
            MethodName = rawMethodStr.Substring(1, fieldStartPos - 1);
            var fieldEndPos = rawMethodStr.IndexOf('>');
            FieldName = rawMethodStr.Substring(fieldStartPos + 1, fieldEndPos - fieldStartPos - 1);
            Matches = matches;
        }

        public MethodLuceneASTNode(string rawMethodStr, TermLuceneASTNode match) : this(rawMethodStr, new List<TermLuceneASTNode>() { match }) { }
        public string MethodName { get; set; }
        public string FieldName { get; set; }
        public List<TermLuceneASTNode> Matches { get; set; }
        public override IEnumerable<LuceneASTNodeBase> Children => Matches;

        public override global::Lucene.Net.Search.Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            configuration.FieldName = new FieldName(FieldName);
            var matchList = new List<string>();
            foreach (var match in Matches)
            {
                matchList.AddRange(match.GetAnalyzedTerm(configuration));
            }
            return new TermsMatchQuery(FieldName, matchList);
        }

        public override string ToString()
        {
            var sb = new StringBuilder(GetPrefixString()).Append("@").Append(MethodName).Append('<').Append(FieldName).Append('>').Append(":(").Append(string.Join(" ,", Matches.Select(x => x.Term))).Append(")");
            return sb.ToString();
        }
    }
}