using System;

namespace Raven.Server.Documents.Queries.AST
{
    public class PatternMatchBinaryExpression : PatternMatchExpression
    {
        public enum Operator
        {
            // ReSharper disable once InconsistentNaming
            And,
            // ReSharper disable once InconsistentNaming
            Or
        }

        public PatternMatchExpression Left;

        public PatternMatchExpression Right;

        public Operator Op;

        public PatternMatchBinaryExpression(PatternMatchExpression left, PatternMatchExpression right, Operator op)
        {
            Left = left ?? throw new ArgumentNullException(nameof(left));
            Right = right ?? throw new ArgumentNullException(nameof(right));
            Op = op;
        }

        public override string ToString()
        {
            return $"{Left} {Op} {Right}";
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            return $"{Left.GetText(parent)} {Op} {Right.GetText(parent)}";
        }
    }
}
