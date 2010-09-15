using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Raven.Client.Linq
{
	/// <summary>
	/// Implements <see cref="IRavenQueryable{T}"/>
	/// </summary>
    public class RavenQueryable<T> : IRavenQueryable<T>
    {
        private readonly Expression expression;
        private readonly IRavenQueryProvider provider;

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenQueryable&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="provider">The provider.</param>
        public RavenQueryable(IRavenQueryProvider provider)
        {
            if (provider == null)
            {
                throw new ArgumentNullException("provider");
            }
            this.provider = provider;
            expression = Expression.Constant(this);
        }

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenQueryable&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="provider">The provider.</param>
		/// <param name="expression">The expression.</param>
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

    	public override string ToString()
        {
            var ravenQueryProvider = new RavenQueryProviderProcessor<T>(provider.Session, null, provider.IndexName);
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