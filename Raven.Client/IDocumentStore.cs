using System;
using Raven.Client.Client;
using Raven.Client.Document;

namespace Raven.Client
{
    public interface IDocumentStore : IDisposable
    {
		event EventHandler<StoredEntityEventArgs> Stored;
		
		string Identifier { get; set; }

        IDocumentStore Initialize();

    	IDocumentStore RegisterListener(IDocumentDeleteListener deleteListener);

		IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener);

        IDocumentSession OpenSession();

        IDatabaseCommands DatabaseCommands { get; }

    	DocumentConvention Conventions { get; }
    }

	public class StoredEntityEventArgs : EventArgs
	{
		public string SessionIdentifier { get; set; }
		public object EntityInstance { get; set; }
	}
}
