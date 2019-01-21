using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Primitives;
using Raven.Client.Exceptions;

namespace Raven.Server.Documents.Queries.AST
{
    public class GraphQuerySyntaxValidatorVisitor : QueryVisitor
    {
        private readonly GraphQuery _query;
        private readonly HashSet<StringSegment> _aliases = new HashSet<StringSegment>(StringSegmentComparer.OrdinalIgnoreCase);

        public GraphQuerySyntaxValidatorVisitor(GraphQuery query)
        {
            _query = query;
        }

        private enum QueryStepElementType
        {
            Vertex,
            Edge
        }
        
        private void ThrowIfDuplicateAlias(MatchPath path)
        {
            if (path.Field != null)
            {
                if (_aliases.Contains(path.Alias) && !path.Alias.Equals(path.Field.FieldValue))
                {
                    var isImplicitAlias = false;
                    if (path.IsEdge && _query.WithEdgePredicates.TryGetValue(path.Alias, out var withEdge))
                    {
                        isImplicitAlias = withEdge.ImplicitAlias;
                    }
                    else if(_query.WithDocumentQueries.TryGetValue(path.Alias, out var withQuery))
                    {
                        isImplicitAlias = withQuery.implicitAlias;
                    }

                    if (isImplicitAlias)
                        throw new InvalidQueryException($"Found redefinition of an implicit alias '{path.Alias}', this is not allowed. Note: If you specify nodes or edges without an alias, for example like this: '(Employees)', then implicit alias will be generated. ", _query.QueryText);

                    throw new InvalidQueryException($"Found redefinition of alias '{path.Alias}', this is not allowed. The correct syntax is to have only single alias definition in the form of '(Employees as e)'", _query.QueryText);
                }

                _aliases.Add(path.Alias);
            }
            else if (!_aliases.Contains(path.Alias) && !_query.WithDocumentQueries.ContainsKey(path.Alias) && !_query.WithEdgePredicates.ContainsKey(path.Alias))
            {
                throw new InvalidQueryException($"Found duplicate alias '{path.Alias}', this is not allowed", _query.QueryText);
            }
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
                ThrowIfDuplicateAlias(elements[0]);

                if (elements[0].IsEdge && !elements[0].Recursive.HasValue)
                    ThrowExpectedVertexButFoundEdge(elements[0],elementExpression);

                if (elements[elements.Length - 1].IsEdge && !elements[elements.Length - 1].Recursive.HasValue)
                    ThrowExpectedVertexButFoundEdge(elements[elements.Length - 1],elementExpression);

                var last = QueryStepElementType.Vertex; //we just verified that the first (or last) is a vertex
                for (var i = 1; i < elements.Length; i++)
                {
                    QueryStepElementType next;                    
                    var nextIsRecursive = false;
                    if (elements[i].Recursive.HasValue)
                    {                        
                        var matchPath = elements[i].Recursive.Value.Pattern;
                        foreach(var path in matchPath )
                            ThrowIfDuplicateAlias(path);

                        next = DetermineEdgeOrVertex(matchPath[0]);
                        nextIsRecursive = true;
                    }
                    else
                    {
                        ThrowIfDuplicateAlias(elements[i]);
                        next = DetermineEdgeOrVertex(elements[i]);
                    }

                    switch (last)
                    {
                        case QueryStepElementType.Vertex when next != QueryStepElementType.Edge:
                            if (nextIsRecursive)
                                ThrowExpectedEdgeButFoundRecursive(elements[i], elementExpression);
                            ThrowExpectedEdgeButFoundVertex(elements[i], elementExpression);
                            break;
                        case QueryStepElementType.Edge when next != QueryStepElementType.Vertex:
                            if (nextIsRecursive)
                                ThrowExpectedVertexButFoundRecursive(elements[i], elementExpression);
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

        private static void ThrowExpectedVertexButFoundRecursive(MatchPath patternMatchElement, PatternMatchElementExpression elementExpression)
        {
            throw new InvalidQueryException($"Invalid pattern match syntax: expected element '{patternMatchElement}' to be of type vertex, but it is a recursive clause. In a graph query, recursive clause is considered an edge, and should be preceded by a vertex and superseded by edge. (The full expression: '{elementExpression}')");
        }

        private static void ThrowExpectedEdgeButFoundVertex(MatchPath patternMatchElement, PatternMatchElementExpression elementExpression)
        {
            throw new InvalidQueryException($"Invalid pattern match syntax: expected element '{patternMatchElement}' to be of type edge, but it is an vertex. (The full expression: '{elementExpression}')");
        }

        private static void ThrowExpectedEdgeButFoundRecursive(MatchPath patternMatchElement, PatternMatchElementExpression elementExpression)
        {
            throw new InvalidQueryException($"Invalid pattern match syntax: expected element '{patternMatchElement}' to be of type edge, but it is a recursive clause. In a graph query, recursive clause is considered an edge, and should be preceded by a vertex and superseded by edge. (The full expression: '{elementExpression}')");
        }
    }
}
