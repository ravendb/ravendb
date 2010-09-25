using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Linq;

namespace Raven.Client.DynamicQueries
{
    public class RavenDynamicQueryProviderProcessor<T> : RavenQueryProviderProcessor<T>
    {
        /// <summary>
		/// Initializes a new instance of the <see cref="RavenDynamicQueryProviderProcessor&lt;T&gt;"/> class.
		/// </summary>
		/// <param name="session">The session.</param>
		/// <param name="customizeQuery">The customize query.</param>
		/// <param name="indexName">Name of the index.</param>
        public RavenDynamicQueryProviderProcessor(
			IDocumentSession session,
			Action<IDocumentQuery<T>> customizeQuery) : base(session, customizeQuery, "dynamic")
		{

		}

        protected override IDocumentQuery<T> CreateDocumentQuery()
        {
            return this.Session.DynamicLuceneQuery<T>();
        }
    }
}
