using System;
using System.Collections.Generic;
using System.Text;
using Raven.Server.Documents.Queries;

namespace Tryouts.GraphAPI
{
    public class PatternMatchElementExpression : PatternMatchExpression
    {
        public enum Direction
        {
            Left,
            Right
        }

        public PatternMatchVertexExpression From;

        public Direction EdgeDirection;

        public PatternMatchExpressEdge Edge;

        public PatternMatchExpression To;

        public override string ToString()
        {
            throw new NotImplementedException();
        }

        public override string GetText(IndexQueryServerSide parent)
        {
            throw new NotImplementedException();
        }
    }
}
