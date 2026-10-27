namespace Raven.Client.Documents.Session;

internal interface IAbstractDocumentQueryAccessor
{
    /// <summary>Adds AND to the query, wrapping the previous clauses first if they have a top-level OR.</summary>
    void AndAlsoWrappingTopLevelOr();
}
