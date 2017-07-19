using System.Collections.Generic;

namespace Raven.Server.Documents.Queries.Parser.Lucene
{
    public class FieldLuceneASTNode : LuceneASTNodeBase
    {
        public FieldName FieldName { get; set; }
        public LuceneASTNodeBase Node { get; set; }
        public override IEnumerable<LuceneASTNodeBase> Children
        {
            get { yield return Node; }
        }
        public override global::Lucene.Net.Search.Query ToQuery(LuceneASTQueryConfiguration configuration)
        {
            configuration.FieldName = FieldName;
            var res = Node.ToGroupFieldQuery(configuration);
            return res;
        }
        public override string ToString()
        {
            return string.Format("{0}{1}:{2}", GetPrefixString(), FieldName, Node);
        }
    }
}