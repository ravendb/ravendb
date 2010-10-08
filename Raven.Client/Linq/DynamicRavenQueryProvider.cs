using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Raven.Client.Linq
{
    /// <summary>
    /// This is a specialised query provider for querying dynamic indexes
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DynamicRavenQueryProvider<T> : RavenQueryProvider<T>
    {
        /// <summary>
        /// Creates a dynamic query provider around the provided document session
        /// </summary>
        /// <param name="session"></param>
        public DynamicRavenQueryProvider(IDocumentSession session) 
            : base(session, "dynamic")     
        {
               
        }

        /// <summary>
        /// A specialized execute method for the dynamic provider
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public override object Execute(System.Linq.Expressions.Expression expression)
        {
            return new DynamicQueryProviderProcessor<T>(this.Session, this.CustomizedQuery).Execute(expression);
        }
    }
}
