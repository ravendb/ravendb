using System;
using System.Linq;

namespace Raven.Client.Linq
{
    public interface IRavenQueryProvider : IQueryProvider
    {
        void Customize(Delegate action);
        IDocumentSession Session { get; }
        string IndexName { get; }
    }
}