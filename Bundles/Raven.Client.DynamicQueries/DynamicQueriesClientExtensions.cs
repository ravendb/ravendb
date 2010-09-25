using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Linq;
using Raven.Client.Document;

namespace Raven.Client.DynamicQueries
{
    public static class DynamicQueriesClientExtensions
    {
        public static IRavenQueryable<T> DynamicQuery<T>(this IDocumentSession session)
        {
            return new RavenQueryable<T>(new RavenDynamicQueryProvider<T>(session));
        }

        public static IDocumentQuery<T> DynamicLuceneQuery<T>(this IDocumentSession session)
        {
            // Argh, why does DocumentQuery take in a concrete instance of this class?
            return new DynamicDocumentQuery<T>((DocumentSession)session, session.DatabaseCommands);
        }
    }
}
