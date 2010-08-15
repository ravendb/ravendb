using System;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Raven.Client.Linq
{
	public class RavenQueryProvider<T> :  IRavenQueryProvider
    {
        private readonly string indexName;
        private Action<IDocumentQuery<T>> customizeQuery;
		private readonly IDocumentSession session;

		public IDocumentSession Session
        {
            get { return session; }
        }

        public string IndexName
        {
            get { return indexName; }
        }

    	public RavenQueryProvider(IDocumentSession session, string indexName)
        {
            this.session = session;
            this.indexName = indexName;
        }

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