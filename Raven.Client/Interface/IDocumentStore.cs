using System;
namespace Raven.Client
{
    public interface IDocumentStore : IDisposable
    {
        void Delete(Guid id);
        IDocumentStore Initialise();
        IDocumentSession OpenSession();
        DocumentConvention Conventions { get; set; }
    }
}
