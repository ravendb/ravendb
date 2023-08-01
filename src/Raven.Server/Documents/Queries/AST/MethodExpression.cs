using System.Collections.Generic;
using System.Linq;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public sealed class MethodExpression : QueryExpression
    {
        private string _text;
        private string _textWithAlias;

        public StringSegment Name;
        public StringSegment Alias;
        public List<QueryExpression> Arguments;

        public MethodExpression(StringSegment name, List<QueryExpression> arguments)
        {
            Name = name;
            Arguments = arguments;
            Type = ExpressionType.Method;
        }
        public override string ToString()
        {
            return Name + "(" + string.Join(", ", Arguments.Select(x => x.ToString())) + ")";
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            return _text ??= $"{Name}({string.Join(", ", Arguments.Select(x => x.GetText(parent)))})";
        }

        public override string GetTextWithAlias(IndexQueryServerSide parent)
        {
            return _textWithAlias ??= $"{Name}({string.Join(", ", Arguments.Select(x => x.GetTextWithAlias(parent)))})";
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is MethodExpression ie))
                return false;

            if (Name != ie.Name || Alias != ie.Alias || Arguments.Count != ie.Arguments.Count)
                return false;

            for (int i = 0; i < Arguments.Count; i++)
            {
                if (Arguments[i].Equals(ie.Arguments.Count) == false)
                    return false;
            }

            return true;
        }
    }
}
