using System;
using System.Collections.Generic;
using System.Text;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public class MatchPatternExpressionVisitor : GraphQueryVisitor
    {
        private readonly Action<PatternMatchVertexExpression, (StringSegment?, StringSegment?), PatternMatchVertexExpression> _visitor;

        public MatchPatternExpressionVisitor(Action<PatternMatchVertexExpression, (StringSegment?, StringSegment?), PatternMatchVertexExpression> visitor)
        {
            _visitor = visitor;
        }

        public override void VisitElementExpression(PatternMatchElementExpression elementExpression) => 
            _visitor(elementExpression.From, (elementExpression.EdgeAlias, elementExpression.EdgeType), elementExpression.To);
    }
}
