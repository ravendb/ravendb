namespace Raven.Server.Documents.Queries.AST
{
    public class PatternMatchNotExpression :  PatternMatchExpression
    {
        public PatternMatchExpression AppliedOn; // not ( pattern match expression )

        public override string ToString()
        {
            return $"not ( {AppliedOn} )";
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            return $"not ( {AppliedOn.GetText(parent)} )";
        }
    }
}
