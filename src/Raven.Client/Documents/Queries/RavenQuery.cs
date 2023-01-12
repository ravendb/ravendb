using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;
using Raven.Client.Documents.Queries.TimeSeries;
using Raven.Client.Documents.Session;
using Sparrow;

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

        public static bool? Include(string path)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static T Include<T>(string path)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static T Include<T>(Expression<Func<T, string>> path)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static bool IncludeTimeSeries(object documentInstance, string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static bool IncludeCounter(object documentInstance, string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static bool IncludeCounters(object documentInstance, string[] nameList)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }
        public static bool IncludeAllCounters(object documentInstance)
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
        
        public static DateTime LastModified<T>(T instance)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static IMetadataDictionary Metadata<T>(T instance)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static T CmpXchg<T>(string key)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static long? Counter(string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static long? Counter(string docId, string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static long? Counter(object documentInstance, string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static ITimeSeriesQueryable TimeSeries(string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static ITimeSeriesQueryable TimeSeries(object documentInstance, string name)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static ITimeSeriesQueryable TimeSeries(object documentInstance, string name, DateTime from, DateTime to)
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static ITimeSeriesQueryable<T> TimeSeries<T>(string name) where T : new()
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static ITimeSeriesQueryable<T> TimeSeries<T>(object documentInstance, string name) where T : new()
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }

        public static ITimeSeriesQueryable<T> TimeSeries<T>(object documentInstance, string name, DateTime from, DateTime to) where T : new()
        {
            throw new NotSupportedException("This method is here for strongly type support of server side call during Linq queries and should never be directly called");
        }
    }
}
