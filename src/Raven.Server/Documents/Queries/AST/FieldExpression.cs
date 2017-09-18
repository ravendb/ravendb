using Sparrow;

namespace Raven.Server.Documents.Queries.Parser
{
    public class FieldExpression : QueryExpression
    {
        public StringSegment Field;

        public FieldExpression(StringSegment field)
        {
            Field = field;
            Type = ExpressionType.Field;
        }
        
        public FieldExpression(StringSegment field, bool escapeChars)
        {
            Field = field;
            Type = ExpressionType.Field;
        }
    }
}
