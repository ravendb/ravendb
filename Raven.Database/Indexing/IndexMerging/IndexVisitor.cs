using System.Collections.Generic;
using ICSharpCode.NRefactory.CSharp;
using Raven.Database.Linq.Ast;

namespace Raven.Database.Indexing.IndexMerging
{
	internal class IndexVisitor : DepthFirstAstVisitor
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

		public override void VisitMemberReferenceExpression(MemberReferenceExpression memberReferenceExpression)
		{
			base.VisitMemberReferenceExpression(memberReferenceExpression);
			indexData.Collection = memberReferenceExpression.MemberName;
		}

        public override void VisitInvocationExpression(InvocationExpression invocationExpression)
        {
            base.VisitInvocationExpression(invocationExpression);

	        var selectExpressions = new Dictionary<string, Expression>();
	        var visitor = new CaptureSelectNewFieldNamesVisitor(false, new HashSet<string>(), selectExpressions);
            invocationExpression.AcceptVisitor(visitor, null);

            var memberReferenceExpression = invocationExpression.Target as MemberReferenceExpression;

            if (memberReferenceExpression == null)
            {

                base.VisitInvocationExpression(invocationExpression);
                return;
            }

            if (memberReferenceExpression.MemberName == "Where")
                indexData.HasWhere = true;

			indexData.SelectExpressions = selectExpressions;
        }
        public override void VisitQuerySelectClause(QuerySelectClause querySelectClause)
        {
	        var selectExpressions = new Dictionary<string, Expression>();
	        var visitor = new CaptureSelectNewFieldNamesVisitor(false, new HashSet<string>(), selectExpressions);
            querySelectClause.AcceptVisitor(visitor, null);

            indexData.SelectExpressions = selectExpressions;
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