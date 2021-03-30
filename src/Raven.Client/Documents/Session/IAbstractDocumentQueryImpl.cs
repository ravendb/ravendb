using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Tokens;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// This is used as an abstraction for the implementation
    /// of a query when passing to other parts of the
    /// query infrastructure. Meant to be internal only, making
    /// this public to allow mocking / instrumentation. 
    /// </summary>
    public interface IAbstractDocumentQueryImpl<T>
    {
        FieldsToFetchToken FieldsToFetchToken { get; set; }

        bool IsProjectInto { get; }

        QueryOperation InitializeQueryOperation();
    }
}
