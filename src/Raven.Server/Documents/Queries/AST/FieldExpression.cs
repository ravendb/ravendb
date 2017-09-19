using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class FieldExpression : QueryExpression
    {
        public bool IsQuoted;
        public StringSegment Field;

        public FieldExpression(StringSegment field,  bool isQuoted)
        {
            IsQuoted = isQuoted;
            Field = field;
            Type = ExpressionType.Field;
        }

        public override string ToString()
        {
            return "<Field>: " + Field;
        }
    }
}
