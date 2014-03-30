using System.Collections.Generic;
using ICSharpCode.NRefactory.CSharp;
using Raven.Database.Linq.Ast;

namespace Raven.Database.Indexing.IndexMerging
{
    public class IndexVisitor : DepthFirstAstVisitor
    {
        private readonly IndexData indexData;

        public IndexVisitor(IndexData indexData)
        {
            this.indexData = indexData;
            indexData.NumberOfFromClauses = 0;
            indexData.SelectExpressions = new Dictionary<string, Expression>();
           
        }

        public override void VisitQueryFromClause(QueryFromClause queryFromClause)
        {
            base.VisitQueryFromClause(queryFromClause);
            indexData.FromExpression = queryFromClause.Expression.Clone();
            indexData.FromIdentifier = queryFromClause.Identifier;
            indexData.NumberOfFromClauses++;

        }

        public override void VisitInvocationExpression(InvocationExpression invocationExpression)
        {
            base.VisitInvocationExpression(invocationExpression);

            var visitor = new CaptureSelectNewFieldNamesVisitor();
            invocationExpression.AcceptVisitor(visitor, null);

            var memberReferenceExpression = invocationExpression.Target as MemberReferenceExpression;

            if (memberReferenceExpression == null)
            {

                base.VisitInvocationExpression(invocationExpression);
                return;
            }

            if (memberReferenceExpression.MemberName == "Where")
                indexData.HasWhere = true;

            indexData.SelectExpressions = visitor.SelectExpressions;
        }
        public override void VisitQuerySelectClause(QuerySelectClause querySelectClause)
        {
            var visitor = new CaptureSelectNewFieldNamesVisitor();
            querySelectClause.AcceptVisitor(visitor, null);

            indexData.SelectExpressions = visitor.SelectExpressions;
            indexData.NumberOfSelectClauses++;
        }
       
        public override void VisitQueryWhereClause(QueryWhereClause queryWhereClause)
        {
            base.VisitQueryWhereClause(queryWhereClause);
            indexData.HasWhere = true;
        }

        public override void VisitQueryOrderClause(QueryOrderClause queryOrderClause)
        {
            base.VisitQueryOrderClause(queryOrderClause);
            indexData.HasOrder = true;
        }
        public override void VisitQueryOrdering(QueryOrdering queryOrdering)
        {
            base.VisitQueryOrdering(queryOrdering);
            indexData.HasOrder = true;
        }
        public override void VisitQueryGroupClause(QueryGroupClause queryGroupClause)
        {
            base.VisitQueryGroupClause(queryGroupClause);
            indexData.HasGroup = true;
        }
        public override void VisitQueryLetClause(QueryLetClause queryLetClause)
        {
            base.VisitQueryLetClause(queryLetClause);
            indexData.HasLet = true;
        }

       
    }
}