namespace Raven.Client.Documents.Session;

internal interface IAbstractDocumentQueryAccessor
{
    /// <summary>Adds AND to the query; if the previous clauses contain an OR outside parentheses, wraps them first so the AND applies to all of them.</summary>
    void AndAlsoWrappingPreviousOrIfNeeded();
}
