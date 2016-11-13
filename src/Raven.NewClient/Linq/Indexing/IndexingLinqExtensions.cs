using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;
using Raven.NewClient.Client.Data;

namespace Raven.NewClient.Client.Linq.Indexing
{
    /// <summary>
    /// Extension methods that adds additional behavior during indexing operations
    /// </summary>
    public static class IndexingLinqExtensions
    {
        /// <summary>
        /// Marker method for allowing complex (multi entity) queries on the server.
        /// </summary>
        public static IEnumerable<TResult> WhereEntityIs<TResult>(this IEnumerable<object> queryable, params string[] names)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Boost the value with the given amount
        /// </summary>
        public static BoostedValue Boost(this object item, float value)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Marker method for allowing complex (multi entity) queries on the server.
        /// </summary>
        public static TResult IfEntityIs<TResult>(this object queryable, string name)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Boost the value with the given amount
        /// </summary>
        public static string StripHtml(this object item)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Safely parses string value to int, when failed default(int) is returned.
        /// </summary>
        public static string ParseInt(this object item)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Safely parses string value to int, when failed defaultValue is returned.
        /// </summary>
        public static string ParseInt(this object item, int defaultValue)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Safely parses string value to double, when failed default(double) is returned.
        /// </summary>
        public static string ParseDouble(this object item)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Safely parses string value to double, when failed defaultValue is returned.
        /// </summary>
        public static string ParseDouble(this object item, double defaultValue)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Safely parses string value to decimal, when failed default(decimal) is returned.
        /// </summary>
        public static string ParseDecimal(this object item)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Safely parses string value to decimal, when failed defaultValue is returned.
        /// </summary>
        public static string ParseDecimal(this object item, decimal defaultValue)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Safely parses string value to short, when failed default(short) is returned.
        /// </summary>
        public static string ParseShort(this object item)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Safely parses string value to short, when failed defaultValue is returned.
        /// </summary>
        public static string ParseShort(this object item, short defaultValue)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Safely parses string value to long, when failed default(long) is returned.
        /// </summary>
        public static string ParseLong(this object item)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }

        /// <summary>
        /// Safely parses string value to long, when failed defaultValue is returned.
        /// </summary>
        public static string ParseLong(this object item, long defaultValue)
        {
            throw new NotSupportedException("This method is provided solely to allow query translation on the server");
        }
    }
}
