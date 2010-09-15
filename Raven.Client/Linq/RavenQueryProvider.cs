using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Raven.Client.Linq
{
	/// <summary>
	/// An implementation of <see cref="IRavenQueryProvider"/>
	/// </summary>
	public class RavenQueryProvider<T> :  IRavenQueryProvider
    {
        private readonly string indexName;
        private Action<IDocumentQuery<T>> customizeQuery;
		private readonly IDocumentSession session;

		/// <summary>
		/// Gets the session.
		/// </summary>
		/// <value>The session.</value>
		public IDocumentSession Session
        {
            get { return session; }
        }

		/// <summary>
		/// Gets the name of the index.
		/// </summary>
		/// <value>The name of the index.</value>
        public string IndexName
        {
            get { return indexName; }
        }

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenQueryProvider&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="session">The session.</param>
		/// <param name="indexName">Name of the index.</param>
    	public RavenQueryProvider(IDocumentSession session, string indexName)
        {
            this.session = session;
            this.indexName = indexName;
        }

		/// <summary>
		/// Executes the query represented by a specified expression tree.
		/// </summary>
		/// <param name="expression">An expression tree that represents a LINQ query.</param>
		/// <returns>
		/// The value that results from executing the specified query.
		/// </returns>
		public object Execute(Expression expression)
		{
			return new RavenQueryProviderProcessor<T>(session, customizeQuery, indexName).Execute(expression);
		}

		IQueryable<S> IQueryProvider.CreateQuery<S>(Expression expression)
        {
            return new RavenQueryable<S>(this, expression);
        }

        IQueryable IQueryProvider.CreateQuery(Expression expression)
        {
            Type elementType = TypeSystem.GetElementType(expression.Type);
            try
            {
                return
                    (IQueryable)
                    Activator.CreateInstance(typeof(RavenQueryable<>).MakeGenericType(elementType),
                                             new object[] { this, expression });
            }
            catch (TargetInvocationException tie)
            {
                throw tie.InnerException;
            }
        }

        S IQueryProvider.Execute<S>(Expression expression)
        {
            return (S)Execute(expression);
        }

        object IQueryProvider.Execute(Expression expression)
        {
            return Execute(expression);
        }

        public void Customize(Delegate action)
        {
            customizeQuery = (Action<IDocumentQuery<T>>)action;
        }

        #region Helpers

		#endregion Helpers
	}
}