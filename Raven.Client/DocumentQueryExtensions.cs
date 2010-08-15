using System;
using System.Linq.Expressions;
using Raven.Database.Extensions;

namespace Raven.Client
{
    public static class DocumentQueryExtensions
    {
        public static IDocumentQuery<T> Include<T>(this IDocumentQuery<T> self, Expression<Func<T,object>> func)
        {
            self.Include(func.ToPropertyPath());
            return self;
        }
    }
}