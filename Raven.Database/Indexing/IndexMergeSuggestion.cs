using ICSharpCode.NRefactory.CSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Database.Linq.Ast;
using Raven.Abstractions.Indexing;

namespace Raven.Database.Indexing
{
    class IndexMergeSuggestions
    {
    }
    
    public class MergeProposal
    {
        
        public List<QueryData> ProposedForMerge = new List<QueryData>();
        public QueryData MergedData { get; set; }

        public string IndexMergeSuggestion { get; set; }


    }
    public class QueryData
    {
        public string FromExpression { get; set; }
        public string FromIdentifier { get; set; }
        public int NumberOfFromClauses { get; set; }

        public Dictionary<string, Expression> SelectExpressions = new Dictionary<string, Expression>();

        public string OriginalQuery { get; set; }
        public bool HasWhere { get; set; }
        //  public string Where { get; set; }
        public int IndexId { get; set; }
        public string IndexName { get; set; }
        public bool IsAlreadyMerged { get; set; }
        public bool IsSuitedForMerge { get; set; }
        public string Comment { get; set; }


    }
    public class Regina_PleaseRenameMeSoonest_Visitor : DepthFirstAstVisitor
    {
        public Regina_PleaseRenameMeSoonest_Visitor()
        {
            NumberOfFromClauses = 0;
            SelectExpressions = new Dictionary<string, Expression>();
            FromExpression = string.Empty;
        }
        public bool HasWhere { get; set; }
        public Dictionary<string, Expression> SelectExpressions { get; set; }
        public string FromIdentifier { get; set; }
        public string FromExpression { get; set; }
        public int NumberOfFromClauses { get; set; }
        public override void VisitQueryFromClause(QueryFromClause queryFromClause)
        {
            base.VisitQueryFromClause(queryFromClause);
            FromExpression = queryFromClause.Expression.ToString();
            FromIdentifier = queryFromClause.Identifier;
            NumberOfFromClauses++;

        }


        public override void VisitQuerySelectClause(QuerySelectClause querySelectClause)
        {
            var visitor = new CaptureSelectNewFieldNamesVisitor();
            querySelectClause.AcceptVisitor(visitor, null);

            // TODO: get the values  if visited both merge required
            SelectExpressions = visitor.SelectExpressions;
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
                HasWhere = true;

			


            // TODO: get the values : if visited both merge required
            SelectExpressions = visitor.SelectExpressions;
        }

        public override void VisitQueryWhereClause(QueryWhereClause queryWhereClause)
        {
            HasWhere = true;
            base.VisitQueryWhereClause(queryWhereClause);
        }
    }


}
