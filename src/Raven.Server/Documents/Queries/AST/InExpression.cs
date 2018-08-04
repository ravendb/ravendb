using System.Collections.Generic;
using System.Linq;

namespace Raven.Server.Documents.Queries.AST
{
    public class InExpression : QueryExpression
    {
        public bool All;
        public QueryExpression Source;
        public List<QueryExpression> Values;

        public InExpression(QueryExpression source, List<QueryExpression> values, bool all)
        {
            All = all;
            Source = source;
            Values = values;
            Type = ExpressionType.In;
        }

        public override string ToString()
        {
            return Source + " in ( " + string.Join(", ", Values.Select(x => x.ToString())) + ")";
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            return $"{Source} in ({string.Join(", ", Values.Select(x => x.GetText(parent)))})";
        }

        public override bool Equals(QueryExpression other)
        {
            if (!(other is InExpression ie))
                return false;

            if (All != ie.All || Source.Equals(ie.Source) == false || 
                Values.Count != ie.Values.Count)
                return false;


            for (int i = 0; i < Values.Count; i++)
            {
                if (Values[i].Equals(ie.Values.Count) == false)
                    return false;
            }

            return true;
        }
    }
}
