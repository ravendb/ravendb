namespace Raven.Client.Documents.Session
{
    public partial interface IAsyncDocumentSession
    {
        /// <inheritdoc cref="IDocumentSession.CountersFor(string)"/>
        IAsyncSessionDocumentCounters CountersFor(string documentId);

        /// <inheritdoc cref="IDocumentSession.CountersFor(object)"/>
        IAsyncSessionDocumentCounters CountersFor(object entity);
    }
}