namespace Raven.Studio.Plugin
{
	using System;
	using System.Collections.Generic;
	using Client;
	using Database.Data;

	public interface IServer
	{
		string Address { get; }
		string Name { get; }
		IAsyncDocumentSession OpenSession();
		IEnumerable<string> Databases { get; }
		string CurrentDatabase { get; }
		void OpenDatabase(string name, Action callback);
		void Connect(Uri serverAddress, Action callback);
		DatabaseStatistics Statistics { get; }
		bool IsInitialized { get; }
		event EventHandler CurrentDatabaseChanged;
		event EventHandler Connected;
	}
}