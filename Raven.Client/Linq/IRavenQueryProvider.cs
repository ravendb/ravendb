using System;
using System.Linq;

namespace Raven.Client.Linq
{
	/// <summary>
	/// Extension for the builtin <see cref="IQueryProvider"/> allowing for Raven specific operations
	/// </summary>
    public interface IRavenQueryProvider : IQueryProvider
    {
		/// <summary>
		/// Customizes the query using the specified action
		/// </summary>
		/// <param name="action">The action.</param>
        void Customize(Delegate action);
		/// <summary>
		/// Gets the session.
		/// </summary>
		/// <value>The session.</value>
        IDocumentSession Session { get; }
		/// <summary>
		/// Gets the name of the index.
		/// </summary>
		/// <value>The name of the index.</value>
        string IndexName { get; }
    }
}