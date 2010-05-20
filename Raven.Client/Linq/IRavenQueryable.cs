using System;
using System.Linq;

namespace Raven.Client.Linq
{
    public interface IRavenQueryable<T> : IOrderedQueryable<T>
    {
        IRavenQueryable<T> Customize(Action<IDocumentQuery<T>> action);
    }
}