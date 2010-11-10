using System;
using System.Linq.Expressions;
using Raven.Database.Data;

namespace Raven.Client.Linq
{
    /// <summary>
    /// A specialized query provider processor for querying dynamic types
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DynamicQueryProviderProcessor<T> : RavenQueryProviderProcessor<T>
    {
        /// <summary>
        /// Creates a dynamic query provider around the provided session
        /// </summary>
       public DynamicQueryProviderProcessor(IDocumentSession session,
            Action<IDocumentQueryCustomization> customizeQuery, Action<QueryResult> afterQueryExecuted, string indexName) 
            : base(session, customizeQuery, afterQueryExecuted, indexName)
        {

        }

        /// <summary>
        /// Gets member info for the specified expression and the path to that expression
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        protected override ExpressionInfo GetMember(System.Linq.Expressions.Expression expression)
        {
            var parameterExpression = expression as ParameterExpression;
            if (parameterExpression != null)
            {
                return new ExpressionInfo(CurrentPath, parameterExpression.Type);
            }

            var memberExpression = GetMemberExpression(expression);

            //for stnadard queries, we take just the last part. Bu for dynamic queries, we take the whole part
            var path = memberExpression.ToString();
            path = path.Substring(path.IndexOf('.') + 1);


            var info = new ExpressionInfo(path, memberExpression.Member.GetMemberType());

            return new ExpressionInfo(
                CurrentPath + info.Path,
                info.Type);
        }
 
        
    
    }
}
