namespace Raven.Studio.Plugin
{
	using System;
	using System.Collections.Generic;
	using Client;

	public interface IServer
	{
		string Address { get; }
		string Name { get; }
		IAsyncDocumentSession OpenSession();
		IEnumerable<string> Databases {get;}
		string CurrentDatabase {get;}
		void Connect(Uri serverAddress);
	}
}