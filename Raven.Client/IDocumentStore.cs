using System;

namespace Raven.Client
{
    public interface IDocumentStore : IDisposable
    {
		event Action<string, object> Stored;
		
		string Identifier { get; set; }

        IDocumentStore Initialise();
        IDocumentSession OpenSession();
    }
}
