using System;
using System.Linq;

namespace Raven.Client.Linq
{
	/// <summary>
	/// An implementation of <see cref="IOrderedQueryable{T}"/> with Raven specific operation
	/// </summary>
    public interface IRavenQueryable<T> : IOrderedQueryable<T>
    {
		/// <summary>
		/// Customizes the query using the specified action
		/// </summary>
		/// <param name="action">The action.</param>
		/// <returns></returns>
        IRavenQueryable<T> Customize(Action<IDocumentQueryCustomization> action);
	}
}