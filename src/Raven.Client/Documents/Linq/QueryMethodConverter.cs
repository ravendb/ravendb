using System;
using System.Linq.Expressions;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Linq
{
    public abstract class QueryMethodConverter
    {
        public struct Parameters<T>
        {
            internal Parameters(MethodCallExpression expression, IAbstractDocumentQuery<T> documentQuery, Action<Expression> visitExpression)
            {
                Expression = expression;
                DocumentQuery = documentQuery;
                VisitExpression = visitExpression;
            }

            public MethodCallExpression Expression { get; }

            public IAbstractDocumentQuery<T> DocumentQuery { get; }

            public Action<Expression> VisitExpression { get; }
        }

        public abstract bool Convert<T>(Parameters<T> parameters);
    }
}
