using System;
using Raven.Client.Documents.Queries;

namespace Raven.Client.Documents.Session
{
    public sealed class CmpXchg : MethodCall
    {
        /// <summary>
        /// Enables the construction of RQL query that specifically retrieve compare exchange values for a given key.
        /// </summary>
        /// <param name="key">The key of the compare exchange.</param>
        [Obsolete("This method is deprecated and will be removed in a future version. Use RavenDocumentQuery.CmpXchg() instead.")]
        public static CmpXchg Value(string key)
        {
            return new CmpXchg
            {
                Args = new object[] { key },
            };
        }
    }
}
