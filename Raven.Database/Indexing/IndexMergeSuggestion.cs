using ICSharpCode.NRefactory.CSharp;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Database.Linq.Ast;
using Raven.Abstractions.Indexing;
using System.IO;

namespace Raven.Database.Indexing
{
    class IndexMergeSuggestions
    {
    }

    public class MergeProposal
    {

        public List<IndexData> ProposedForMerge = new List<IndexData>();
        public IndexData MergedData { get; set; }

        public string IndexMergeSuggestion { get; set; }


    }
    public class IndexData
    {
        public Expression FromExpression { get; set; }
        public string FromIdentifier { get; set; }
        public int NumberOfFromClauses { get; set; }
        public int NumberOfSelectClauses { get; set; }


        public Dictionary<string, Expression> SelectExpressions = new Dictionary<string, Expression>();
        public bool IsMapReduced { get; set; }
        public bool ExistsMorethan1Maps { get; set; }
        public string OriginalMap { get; set; }
        public bool HasWhere { get; set; }
        public bool HasLet { get; set; }
        public bool HasGroup { get; set; }
        public bool HasOrder { get; set; }

        public int IndexId { get; set; }
        public string IndexName { get; set; }
        public bool IsAlreadyMerged { get; set; }
        public bool IsSuitedForMerge { get; set; }
        public string Comment { get; set; }

        public IDictionary<string, FieldStorage> Stores { get; set; }

        public IDictionary<string, FieldIndexing> Indexes { get; set; }

        public IDictionary<string, SortOptions> SortOptions { get; set; }

        public IDictionary<string, string> Analyzers { get; set; }

        public IDictionary<string, SuggestionOptions> Suggestions { get; set; }

        public IDictionary<string, FieldTermVector> TermVectors { get; set; }

        public IDictionary<string, SpatialOptions> SpatialIndexes { get; set; }



        public IndexData()
        {
            Indexes = new Dictionary<string, FieldIndexing>();
            Stores = new Dictionary<string, FieldStorage>();
            Analyzers = new Dictionary<string, string>();
            SortOptions = new Dictionary<string, SortOptions>();
            Suggestions = new Dictionary<string, SuggestionOptions>();
            TermVectors = new Dictionary<string, FieldTermVector>();
            SpatialIndexes = new Dictionary<string, SpatialOptions>();

        }
        public void FillAdditionalProperies(IndexDefinition index)
        {
            if (index.Indexes.Count > 0)
            {
                Indexes = index.Indexes;
            }
           
            if (index.Stores.Count > 0)
            {
                Stores = index.Stores;
             }

            if (index.Analyzers.Count > 0)
            {
                Analyzers = index.Analyzers;
              }

            if (index.SortOptions.Count > 0)
            {
                SortOptions = index.SortOptions;
             }

            if (index.Suggestions.Count > 0)
            {
                Suggestions = index.Suggestions;
             }
            if (index.TermVectors.Count > 0)
            {
                TermVectors = index.TermVectors;

            }

            if (index.SpatialIndexes.Count > 0)
            {
                SpatialIndexes = index.SpatialIndexes;
             }

        }
       public  string BuildExpression()
        {
            var anonymousTypeCreateExpression = new AnonymousTypeCreateExpression();
            var crrv = new ChangeRootReferenceVisitor(FromIdentifier);
            foreach (var curExpr in SelectExpressions.OrderBy(x=>x.Key))
            {
                curExpr.Value.AcceptVisitor(crrv);
                anonymousTypeCreateExpression.Initializers.Add(
                      new AssignmentExpression(new IdentifierExpression(curExpr.Key), curExpr.Value.Clone()));
            }
            if(FromExpression==null)
                FromExpression = new IdentifierExpression();

            var queryExpr = new QueryExpression
            {
                Clauses =
                {
                    new QueryFromClause
                    {
                        Identifier = "doc",
                        Expression = FromExpression.Clone()
                    },
                    new QuerySelectClause
                    {
                        Expression = anonymousTypeCreateExpression.Clone()
                    }
                }
            };
           FromIdentifier = "doc";

            var printer = new StringWriter();
            var printerVisitor = new CSharpOutputVisitor(printer, FormattingOptionsFactory.CreateSharpDevelop());
            queryExpr.AcceptVisitor(printerVisitor);
            return printer.GetStringBuilder().ToString();


        }

    }
    public class IndexVisitor : DepthFirstAstVisitor
    {
        public IndexVisitor()
        {
            NumberOfFromClauses = 0;
            SelectExpressions = new Dictionary<string, Expression>();
           
        }
       

        public Dictionary<string, Expression> SelectExpressions { get; set; }
        public string FromIdentifier { get; set; }

        public Expression FromExpression { get; set; }
        public int NumberOfFromClauses { get; set; }
        public int NumberOfSelectClauses { get; set; }
        public bool HasWhere { get; set; }
        public bool HasLet { get; set; }
        public bool HasGroup { get; set; }
        public bool HasOrder { get; set; }


        public override void VisitQueryFromClause(QueryFromClause queryFromClause)
        {
            base.VisitQueryFromClause(queryFromClause);
            FromExpression = queryFromClause.Expression.Clone();
            FromIdentifier = queryFromClause.Identifier;
            NumberOfFromClauses++;

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
        public override void VisitQuerySelectClause(QuerySelectClause querySelectClause)
        {
            var visitor = new CaptureSelectNewFieldNamesVisitor();
            querySelectClause.AcceptVisitor(visitor, null);

            // TODO: get the values  if visited both merge required
            SelectExpressions = visitor.SelectExpressions;
            NumberOfSelectClauses++;
        }
       
        public override void VisitQueryWhereClause(QueryWhereClause queryWhereClause)
        {
            base.VisitQueryWhereClause(queryWhereClause);
            HasWhere = true;
        }

        public override void VisitQueryOrderClause(QueryOrderClause queryOrderClause)
        {
            base.VisitQueryOrderClause(queryOrderClause);
            HasOrder = true;
        }
        public override void VisitQueryOrdering(QueryOrdering queryOrdering)
        {
            base.VisitQueryOrdering(queryOrdering);
            HasOrder = true;
        }
        public override void VisitQueryGroupClause(QueryGroupClause queryGroupClause)
        {
            base.VisitQueryGroupClause(queryGroupClause);
            HasGroup = true;
        }
        public override void VisitQueryLetClause(QueryLetClause queryLetClause)
        {
            base.VisitQueryLetClause(queryLetClause);
            HasLet = true;
        }

       
    }

    public class ChangeRootReferenceVisitor : DepthFirstAstVisitor
    {
        private readonly string _fromIdentifier;

        public ChangeRootReferenceVisitor(string fromIdentifier)
        {
            _fromIdentifier = fromIdentifier;
        }

        public override void VisitMemberReferenceExpression(MemberReferenceExpression memberReferenceExpression)
        {
            var identifierExpression = memberReferenceExpression.Target as IdentifierExpression;
            if (identifierExpression != null && identifierExpression.Identifier == _fromIdentifier)
            {
                memberReferenceExpression.Target = new IdentifierExpression("doc");
            }

            base.VisitMemberReferenceExpression(memberReferenceExpression);
        }
    }

}
