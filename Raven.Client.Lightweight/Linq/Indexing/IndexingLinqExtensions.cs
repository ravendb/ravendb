using System;
using System.Collections.Generic;
using Raven.Abstractions.Data;

namespace Raven.Client.Linq.Indexing
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
	}
}