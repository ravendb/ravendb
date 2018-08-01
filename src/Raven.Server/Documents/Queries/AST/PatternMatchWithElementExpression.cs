using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class PatternMatchElementExpression : PatternMatchExpression
    {
     
        public PatternMatchVertexExpression From;

        public StringSegment? EdgeAlias;

        public StringSegment? EdgeType;

        public PatternMatchVertexExpression To;

        public override string ToString() => GetText();

        public override string GetText(IndexQueryServerSide parent) => GetText();
        
        public string GetText() => $"{From}-{GetEdgeText()}->{To}";

        public string GetEdgeText()
        {
            if (EdgeAlias.HasValue && EdgeType.HasValue)
                return $"[{EdgeAlias}:{EdgeType}]";

            if(EdgeAlias.HasValue && !EdgeType.HasValue)
                return $"[{EdgeAlias}]";

            return $"[:{EdgeType}]";
        }
    }
}
