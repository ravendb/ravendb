using System;
namespace Raven.Client
{
    public interface IDocumentStore : IDisposable
    {
        event Action<string, int, object> Stored;
        void Delete(Guid id);
        IDocumentStore Initialise();
        IDocumentSession OpenSession();
        DocumentConvention Conventions { get; set; }
    }
}
