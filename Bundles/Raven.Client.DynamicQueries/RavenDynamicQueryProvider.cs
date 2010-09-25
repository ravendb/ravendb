using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Linq;

namespace Raven.Client.DynamicQueries
{
    public class RavenDynamicQueryProvider<T> : RavenQueryProvider<T>
    {
        private Action<IDocumentQuery<T>> customizeQuery;

        // Mismatch here, need to split create a common based between RavenQueryProvider and RavenDynamicQueryProvider
        // Instead of just inheriting from RavenQueryProvider
        public RavenDynamicQueryProvider(IDocumentSession session)
            : base(session, "dynamic")
        {
        }



        public override object Execute(System.Linq.Expressions.Expression expression)
        {
            return new RavenDynamicQueryProviderProcessor<T>(this.Session, customizeQuery).Execute(expression);
        }

        public override void Customize(Delegate action)
        {
            customizeQuery = (Action<IDocumentQuery<T>>)action;
        }


    }
}
