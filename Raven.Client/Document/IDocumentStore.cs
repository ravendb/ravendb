using System;

namespace Raven.Client.Document
{
    public interface IDocumentStore : IDisposable
    {
        string Identifier { get; set; }
        event Action<string, int, object> Stored;
        IDocumentStore Initialise();
        IDocumentSession OpenSession();
    }
}
