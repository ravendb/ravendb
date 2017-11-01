using System;
using System.Collections.Generic;
using Raven.Client.Documents.Session;

namespace Raven.Client.Documents.Queries
{
    public class RavenQuery
    {
        public static T Load<T>(string id)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static IEnumerable<T> Load<T>(IEnumerable<string> ids)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static T Raw<T>(string js)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static T Raw<T>(T path, string js)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static IMetadataDictionary Metadata<T>(T instance)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static T CmpXchgValue<T>(string key)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        } 
        
        public static bool CmpXchgMatch<T>(string key, T value)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }
    }
}
