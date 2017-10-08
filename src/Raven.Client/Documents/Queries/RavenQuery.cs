using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.Queries
{
    public class RavenQuery
    {
        public static T Load<T>(string id)
        {
            throw new NotSupportedException("Never called");
        }

        public static IEnumerable<T> Load<T>(IEnumerable<string> ids)
        {
            throw new NotSupportedException("Never called");
        }

    }
}
