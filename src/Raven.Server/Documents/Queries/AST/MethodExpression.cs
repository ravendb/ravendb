using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Primitives;

namespace Raven.Server.Documents.Queries.AST
{
    public class MethodExpression : QueryExpression
    {
        private string _text;

        public StringSegment Name;
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
            return _text ?? (_text = $"{Name}({string.Join(", ", Arguments.Select(x => x.GetText(parent)))})");
        }


        public override bool Equals(QueryExpression other)
        {
            if (!(other is MethodExpression ie))
                return false;

            if (Name != ie.Name ||
                Arguments.Count != ie.Arguments.Count)
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
