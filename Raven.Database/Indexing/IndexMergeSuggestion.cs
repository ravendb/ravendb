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
        
        public List<IndexData> ProposedForMerge = new List<IndexData>();
        public IndexData MergedData { get; set; }

        public string IndexMergeSuggestion { get; set; }


    }
    public class IndexData
    {
        public string FromExpression { get; set; }
        public string FromIdentifier { get; set; }
        public int NumberOfFromClauses { get; set; }

        public Dictionary<string, Expression> SelectExpressions = new Dictionary<string, Expression>();
        public bool IsMapReduced { get; set; }
        public bool ExistsMorethan1Maps { get; set; }
        public string OriginalMap { get; set; }
        public bool HasWhere { get; set; }
        public int IndexId { get; set; }
        public string IndexName { get; set; }
        public bool IsAlreadyMerged { get; set; }
        public bool IsSuitedForMerge { get; set; }
        public string Comment { get; set; }
        public bool IsIndexesDefined { get; set; }
        public bool IsStoreDefined { get; set; }
        public bool IsAnalyzerDefined { get; set; }
        public bool IsSortOptionDefined { get; set; }
        public bool IsSuggestionDefined { get; set; }
        public bool IsTermVectorDefined { get; set; }
        public bool IsSpatialIndexDefined { get; set; }

        public IDictionary<string, FieldStorage> Stores { get; set; }

          public IDictionary<string, FieldIndexing> Indexes { get; set; }

          public IDictionary<string, SortOptions> SortOptions { get; set; }

          public IDictionary<string, string> Analyzers { get; set; }

          public IList<string> Fields { get; set; }

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


            Fields = new List<string>();

        }
        public void FillAdditionalProperies(IndexDefinition index)
        {


                Fields = index.Fields;  
                if (index.Indexes.Count > 0)
                {
                    Indexes = index.Indexes;
                    IsIndexesDefined = true;
                }
                else
                {
                    IsIndexesDefined = false;
                }

                if (index.Stores.Count > 0)
                {
                    Stores = index.Stores;
                    IsStoreDefined = true;
                }
                else
                {
                    IsStoreDefined = false;
                }
                
                if (index.Analyzers.Count > 0)
                 {
                    Analyzers = index.Analyzers;
                    IsAnalyzerDefined = true;
                 }
                else
                {
                    IsAnalyzerDefined = false;
                }

                if (index.SortOptions.Count > 0)
                {
                    SortOptions = index.SortOptions;
                    IsSortOptionDefined = true;
                }
                else
                {
                    IsSortOptionDefined = false;
                }

                if (index.Suggestions.Count > 0)
                {
                    Suggestions =index.Suggestions;
                    IsSuggestionDefined = true;
                }
                else
                {
                    IsSuggestionDefined = false;
                }
                if (index.TermVectors.Count > 0)
                {
                   TermVectors =index.TermVectors;
                   IsTermVectorDefined = true;
                }
                else
                {
                  IsTermVectorDefined = false;

                }

                if (index.SpatialIndexes.Count > 0)
                {
                    SpatialIndexes =index.SpatialIndexes;
                    IsSpatialIndexDefined = true;
                }
                else
                {
                    IsSpatialIndexDefined = false;
                }
 
        }
    

    }
    public class IndexVisitor : DepthFirstAstVisitor
    {
        public IndexVisitor()
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
