namespace Raven.Server.Documents.Queries.AST
{
    public class PatternMatchElementExpression : PatternMatchExpression
    {
        public enum Direction
        {
            Left,
            Right
        }

        public PatternMatchVertexExpression From;

        public Direction EdgeDirection;

        public PatternMatchExpressEdge Edge;

        public PatternMatchExpression To;

        public override string ToString() => GetText();

        public override string GetText(IndexQueryServerSide parent) => GetText();
        
        public string GetText() => 
            EdgeDirection == Direction.Right ? $"{From} -{Edge}-> {To}" : $"{To} <-{Edge}- {From}";
    }
}
