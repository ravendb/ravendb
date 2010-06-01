using System;
using Raven.Client.Client;

namespace Raven.Client
{
    public interface IDocumentStore : IDisposable
    {
		event Action<string, object> Stored;
		
		string Identifier { get; set; }

        IDocumentStore Initialize();
        
        IDocumentSession OpenSession();

        IDatabaseCommands DatabaseCommands { get; }
    }
}
