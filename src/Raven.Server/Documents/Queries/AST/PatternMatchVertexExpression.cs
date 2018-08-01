using System;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class PatternMatchVertexExpression : PatternMatchExpression
    {
        public readonly StringSegment? Alias;

        public readonly StringSegment? VertexType;

        protected bool Equals(PatternMatchVertexExpression other)
        {
            return Alias.Equals(other.Alias) && VertexType.Equals(other.VertexType);
        }

        public override bool Equals(object obj)
        {
            if (ReferenceEquals(null, obj)) return false;
            if (ReferenceEquals(this, obj)) return true;

            return obj.GetType() == GetType() && 
                   Equals((PatternMatchVertexExpression)obj);
        }

        public override int GetHashCode()
        {
            unchecked
            {
                return ((Alias?.GetHashCode()).GetValueOrDefault() * 397) ^ (VertexType?.GetHashCode()).GetValueOrDefault();
            }
        }

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
                return $"({VertexType})";

            return $"({Alias}:{VertexType})";
        }
    }
}
