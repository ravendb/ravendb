using System;
using Raven.Server.Documents.Queries;
using Sparrow;

namespace Tryouts.GraphAPI
{
    public class PatternMatchExpressEdge : PatternMatchExpression
    {
        public StringSegment? Alias;

        public StringSegment? EdgeType;

        public PatternMatchExpression Predicate; //TODO : finish defining this =>  -[:HasRating(rating >= 3)]-> 

        public PatternMatchExpressEdge(StringSegment? @alias, StringSegment? edgeType, PatternMatchExpression predicate = null)
        {
            if(!@alias.HasValue && !edgeType.HasValue)
                throw new ArgumentNullException($"{nameof(PatternMatchExpressEdge )} cannot have both vertex type and alias null");

            Alias = alias;
            EdgeType = edgeType;
            Predicate = predicate;
        }

        public override string ToString() => GetText();

        public override string GetText(IndexQueryServerSide parent) => GetText();

        private string GetText()
        {
            if (Alias.HasValue && !EdgeType.HasValue)
                return $"[{Alias}]";

            var predicateText = Predicate?.ToString() ?? string.Empty;

            if (!Alias.HasValue && EdgeType.HasValue)
                return $"[:{EdgeType}{predicateText}]";

            return $"[{Alias}:{EdgeType}{predicateText}]";
        }
    }
}
