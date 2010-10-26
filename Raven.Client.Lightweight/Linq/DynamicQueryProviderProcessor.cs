using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database.Data;

namespace Raven.Client.Linq
{
    /// <summary>
    /// A specialised query provider processor for querying dynamic types
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DynamicQueryProviderProcessor<T> : RavenQueryProviderProcessor<T>
    {
        /// <summary>
        /// Creates a dynamic query proivder around the provided session
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
        protected override ExpressionMemberInfo GetMember(System.Linq.Expressions.Expression expression)
        {
            var memberExpression = base.GetMemberExpression(expression);

            //for stnadard queries, we take just the last part. Bu for dynamic queries, we take the whole part
            var path = memberExpression.ToString();
            path = path.Substring(path.IndexOf('.') + 1);


            var info = new ExpressionMemberInfo(path, memberExpression.Member);

            return new ExpressionMemberInfo(
                this.CurrentPath + info.Path,
                info.InnerMemberInfo);
        }
 
        
    
    }
}
