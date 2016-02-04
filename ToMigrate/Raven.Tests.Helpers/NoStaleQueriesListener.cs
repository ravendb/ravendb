using Raven.Client;
using Raven.Client.Listeners;

namespace Raven.Tests.Helpers
{
    /// <summary>
    /// Forces all document session queryies to wait for non stale results.
    /// WARNING: This should not be used in production - it's designed for
    /// use with unit/integration tests.
    /// </summary>
    internal class NoStaleQueriesListener : IDocumentQueryListener
    {
        #region IDocumentQueryListener Members

        /// <summary>
        /// Allow to customize a query globally.
        /// </summary>
        /// <param name="queryCustomization">Customize the document query.</param>
        public void BeforeQueryExecuted(IDocumentQueryCustomization queryCustomization)
        {
            queryCustomization.WaitForNonStaleResults();
        }

        #endregion
    }
}
