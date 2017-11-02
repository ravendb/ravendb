namespace Raven.Client.Documents.Session
{
    public interface IGroupByDocumentQuery<T>
    {
        IGroupByDocumentQuery<T> SelectKey(string fieldName = null, string projectedName = null);

        IDocumentQuery<T> SelectSum(GroupByField field, params GroupByField[] fields);

        IDocumentQuery<T> SelectCount(string projectedName = null);
    }

    public interface IAsyncGroupByDocumentQuery<T>
    {
        IAsyncGroupByDocumentQuery<T> SelectKey(string fieldName = null, string projectedName = null);

        IAsyncDocumentQuery<T> SelectSum(GroupByField field, params GroupByField[] fields);

        IAsyncDocumentQuery<T> SelectCount(string projectedName = null);
    }
}
