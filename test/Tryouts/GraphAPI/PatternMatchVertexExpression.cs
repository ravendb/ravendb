using System;
using Raven.Server.Documents.Queries;
using Sparrow;

namespace Tryouts.GraphAPI
{
    public class PatternMatchVertexExpression : PatternMatchExpression
    {
        public StringSegment? Alias;

        public StringSegment? VertexType;

        public PatternMatchVertexExpression(StringSegment? @alias, StringSegment? vertexType)
        {
            if(!@alias.HasValue && !vertexType.HasValue)
                throw new ArgumentNullException($"{nameof(PatternMatchVertexExpression )} cannot have both vertex type and alias null");

            Alias = alias;
            VertexType = vertexType;
        }

        public override string ToString() => GetText();
        public override string GetText(IndexQueryServerSide parent) => GetText();

        private string GetText()
        {
            if (Alias.HasValue && !VertexType.HasValue)
                return $"({Alias})";

            if (!Alias.HasValue && VertexType.HasValue)
                return $"(:{VertexType})";

            return $"({Alias}:{VertexType})";
        }
    }
}
