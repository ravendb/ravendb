using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Linq.Expressions;
using System.Reflection;

namespace Raven.Client.Linq
{
    /// <summary>
    /// This is a specialised query provider for querying dynamic indexes
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class DynamicRavenQueryProvider<T> : IRavenQueryProvider
    {        
        private Action<IDocumentQueryCustomization> customizeQuery;
		private readonly IDocumentSession session;
        
        /// <summary>
        /// Gets the IndexName for this dynamic query provider (always "dynamic" in this case)
        /// </summary>
        public string IndexName
        {
            get { return "dynamic"; }
        }

        /// <summary>
        /// Creates a dynamic query provider around the provided document session
        /// </summary>
        /// <param name="session"></param>
        public DynamicRavenQueryProvider(IDocumentSession session)  
        {
            this.session = session;
        }

        /// <summary>
        /// Gets the actions for customising the generated lucene query
        /// </summary>
        public Action<IDocumentQueryCustomization> CustomizedQuery
        {
            get { return customizeQuery; }
        }

		/// <summary>
		/// Gets the session.
		/// </summary>
		/// <value>The session.</value>
		public IDocumentSession Session
        {
            get { return session; }
        }

	    /// <summary>
	    /// Change the result type for the query provider
	    /// </summary>
	    public IRavenQueryProvider For<S>()
	    {
            if (typeof(T) == typeof(S))
                return this;

	        var ravenQueryProvider = new DynamicRavenQueryProvider<S>(session);
	        ravenQueryProvider.Customize(customizeQuery);
	        return ravenQueryProvider;
	    }

		/// <summary>
		/// Executes the query represented by a specified expression tree.
		/// </summary>
		/// <param name="expression">An expression tree that represents a LINQ query.</param>
		/// <returns>
		/// The value that results from executing the specified query.
		/// </returns>
		public virtual object Execute(Expression expression)
		{
			return new DynamicQueryProviderProcessor<T>(session, customizeQuery).Execute(expression);
		}

		IQueryable<S> IQueryProvider.CreateQuery<S>(Expression expression)
        {
            return new DynamicRavenQueryable<S>(this, expression);
        }

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            Type elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return
                    (IQueryable)
                    Activator.CreateInstance(typeof(DynamicRavenQueryable<>).MakeGenericType(elementType),
                                             new object[] { this, expression });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

		/// <summary>
		/// Executes the specified expression.
		/// </summary>
		/// <typeparam name="S"></typeparam>
		/// <param name="expression">The expression.</param>
		/// <returns></returns>
        S IQueryProvider.Execute<S>(Expression expression)
        {
            return (S)Execute(expression);
        }

		/// <summary>
		/// Executes the query represented by a specified expression tree.
		/// </summary>
		/// <param name="expression">An expression tree that represents a LINQ query.</param>
		/// <returns>
		/// The value that results from executing the specified query.
		/// </returns>
        object IQueryProvider.Execute(Expression expression)
        {
            return Execute(expression);
        }

		/// <summary>
		/// Customizes the query using the specified action
		/// </summary>
		/// <param name="action">The action.</param>
        public virtual void Customize(Action<IDocumentQueryCustomization> action)
        {
            if (action == null)
                return;
            customizeQuery += action;
        }
    }
}
