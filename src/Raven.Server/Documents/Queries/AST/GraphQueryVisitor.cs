using System;
using System.Collections.Generic;
using Sparrow;

namespace Raven.Server.Documents.Queries.AST
{
    public abstract class GraphQueryVisitor : QueryVisitor
    {
        public void Visit(GraphQuery q)
        {
            if (q.WithDocumentQueries != null)
                VisitWithClauses(q.WithDocumentQueries);

            if(q.WithEdgePredicates != null)
                VisitWithEdgePredicates(q.WithEdgePredicates);

            if(q.MatchClause != null)
                VisitPatternMatchClause(q.MatchClause);

            if(q.Include != null)
                VisitInclude(q.Include);

            if(q.OrderBy != null)
                VisitOrderBy(q.OrderBy);

            if(q.DeclaredFunctions != null)
                VisitDeclaredFunctions(q.DeclaredFunctions);

            if (q.SelectFunctionBody.FunctionText != null)
                VisitSelectFunctionBody(q.SelectFunctionBody.FunctionText);
        }

        public virtual void VisitWithClauses(Dictionary<StringSegment, Query> expression)
        {
            foreach(var withClause in expression)
                Visit(withClause.Value);
        }

        public virtual void VisitWithEdgePredicates(Dictionary<StringSegment, WithEdgesExpression> expression)
        {
            foreach(var withEdgesClause in expression)
                VisitWithEdgesExpression(withEdgesClause.Value);
        }

        public virtual void VisitWithEdgesExpression(WithEdgesExpression expression)
        {
            if(expression.Where != null)
                VisitWhereClause(expression.Where);

            if(expression.OrderBy != null)
                VisitOrderBy(expression.OrderBy);
        }

        public virtual void VisitPatternMatchClause(PatternMatchExpression expression)
        {            
            switch (expression)
            {
                case PatternMatchBinaryExpression binaryExpression:
                    VisitBinaryExpression(binaryExpression);
                    break;
                case PatternMatchElementExpression elementExpression:
                    VisitElementExpression(elementExpression);
                    break;
            }
        }


        public virtual void VisitElementExpression(PatternMatchElementExpression elementExpression)
        {
        }

        public virtual void VisitBinaryOperator(PatternMatchBinaryExpression binaryExpression, PatternMatchBinaryExpression.Operator op)
        {
        }

        public virtual void VisitBinaryExpression(PatternMatchBinaryExpression binaryExpression)
        {
            VisitPatternMatchClause(binaryExpression.Left);
            VisitBinaryOperator(binaryExpression, binaryExpression.Op);
            VisitPatternMatchClause(binaryExpression.Right);
        }
    }
}
