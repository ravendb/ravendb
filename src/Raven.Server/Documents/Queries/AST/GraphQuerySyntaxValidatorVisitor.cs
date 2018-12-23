using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using Raven.Client.Exceptions;

namespace Raven.Server.Documents.Queries.AST
{
    public class GraphQuerySyntaxValidatorVisitor : QueryVisitor
    {
        public static GraphQuerySyntaxValidatorVisitor Instance { get; } = new GraphQuerySyntaxValidatorVisitor();

        private enum QueryStepElementType
        {
            Vertex,
            Edge
        }

        public override void VisitPatternMatchElementExpression(PatternMatchElementExpression elementExpression)
        {
            var elements = elementExpression.Path;
            if (elements.Length == 1)
            {
                if (elements[0].IsEdge)
                    ThrowExpectedVertexButFoundEdge(elements[0],elementExpression);
            }
            else 
            {
                if (elements[0].IsEdge && !elements[0].Recursive.HasValue)
                    ThrowExpectedVertexButFoundEdge(elements[0],elementExpression);

                if (elements[elements.Length - 1].IsEdge && !elements[elements.Length - 1].Recursive.HasValue)
                    ThrowExpectedVertexButFoundEdge(elements[elements.Length - 1],elementExpression);

                var last = QueryStepElementType.Vertex; //we just verified that the first (or last) is a vertex
                for (var i = 1; i < elements.Length; i++)
                {
                    QueryStepElementType next;
                    if (elements[i].Recursive.HasValue)
                    {
                        var matchPath = elements[i].Recursive.Value.Pattern;
                        next = DetermineEdgeOrVertex(matchPath[0]);
                    }
                    else
                    {
                        next = DetermineEdgeOrVertex(elements[i]);
                    }

                    switch (last)
                    {
                        case QueryStepElementType.Vertex when next != QueryStepElementType.Edge:
                            ThrowExpectedEdgeButFoundVertex(elements[i], elementExpression);
                            break;
                        case QueryStepElementType.Edge when next != QueryStepElementType.Vertex:
                            ThrowExpectedVertexButFoundEdge(elements[i], elementExpression);
                            break;
                    }

                    if (elements[i].Recursive.HasValue)
                    {
                        var matchPath = elements[i].Recursive.Value.Pattern;
                        last = DetermineEdgeOrVertex(matchPath[matchPath.Count - 1]);
                    }
                    else
                        last = next;
                }
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static QueryStepElementType DetermineEdgeOrVertex(MatchPath patternMatchElement) => patternMatchElement.IsEdge ? QueryStepElementType.Edge : QueryStepElementType.Vertex;       

        private static void ThrowExpectedVertexButFoundEdge(MatchPath patternMatchElement, PatternMatchElementExpression elementExpression)
        {
            throw new InvalidQueryException($"Invalid pattern match syntax: expected element '{patternMatchElement}' to be of type vertex, but it is an edge. (The full expression: '{elementExpression}')");
        }

        private static void ThrowExpectedEdgeButFoundVertex(MatchPath patternMatchElement, PatternMatchElementExpression elementExpression)
        {
            throw new InvalidQueryException($"Invalid pattern match syntax: expected element '{patternMatchElement}' to be of type edge, but it is an vertex. (The full expression: '{elementExpression}')");
        }
    }
}
