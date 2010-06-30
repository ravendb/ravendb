using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using Raven.Database.Data;

namespace Raven.Client.Linq
{
    public class RavenQueryable<T> : IRavenQueryable<T>
    {
        private readonly Expression expression;
        private readonly IRavenQueryProvider provider;

        public RavenQueryable(IRavenQueryProvider provider)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("provider");
            }
            this.provider = provider;
            expression = Expression.Constant(this);
        }

        public RavenQueryable(IRavenQueryProvider provider, Expression expression)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("provider");
            }
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }
            if (!typeof(IQueryable<T>).IsAssignableFrom(expression.Type))
            {
                throw new ArgumentOutOfRangeException("expression");
            }
            this.provider = provider;
            this.expression = expression;
        }

        #region IOrderedQueryable<T> Members

        Expression IQueryable.Expression
        {
            get { return expression; }
        }

        Type IQueryable.ElementType
        {
            get { return typeof(T); }
        }

        IQueryProvider IQueryable.Provider
        {
            get { return provider; }
        }

        public IEnumerator<T> GetEnumerator()
        {
            return ((IEnumerable<T>)provider.Execute(expression)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)provider.Execute(expression)).GetEnumerator();
        }

        #endregion

        public IRavenQueryable<T> Customize(Action<IDocumentQuery<T>> action)
        {
            provider.Customize(action);
            return this;
        }

    	public QueryResult QueryResult
    	{
    		get { return provider.QueryResult; }
    	}

    	public override string ToString()
        {
            var ravenQueryProvider = new RavenQueryProvider<T>(provider.Session, provider.IndexName);
            ravenQueryProvider.ProcessExpression(expression);
            string fields = "";
            if (ravenQueryProvider.FieldsToFetch.Count > 0)
                fields = "<" + string.Join(", ", ravenQueryProvider.FieldsToFetch.ToArray()) + ">: ";
            return 
                fields + 
                ravenQueryProvider.LuceneQuery;
        }
    }
}