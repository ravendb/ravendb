using System;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class PatternMatchExpressEdge : PatternMatchExpression
    {
        public StringSegment? Alias;

        public StringSegment? EdgeType;

        //this is the predicate inside the []
        //for example, for an edge  -[:HasRating(rating >= 3)]-> 
        //the predicate will have "rating >= 3"

        public PatternMatchExpressEdge(StringSegment? @alias, StringSegment? edgeType)
        {
            if(!@alias.HasValue && !edgeType.HasValue)
                throw new ArgumentNullException($"{nameof(PatternMatchExpressEdge )} cannot have both vertex type and alias null");

            Alias = alias;
            EdgeType = edgeType;
        }

        public override string ToString() => GetText();

        public override string GetText(IndexQueryServerSide parent) => GetText();

        private string GetText()
        {
            if (Alias.HasValue && !EdgeType.HasValue)
                return $"[{Alias}]";

            if (!Alias.HasValue && EdgeType.HasValue)
                return $"[:{EdgeType}]";

            return $"[{Alias}:{EdgeType}]";
        }
    }
}
