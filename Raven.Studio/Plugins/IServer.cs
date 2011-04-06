namespace Raven.Studio.Plugins
{
	using System;
	using System.Collections.Generic;
	using Client;
	using Database.Data;
	using Statistics;

	public interface IServer
	{
		string Address { get; }
		string Name { get; }
		IEnumerable<string> Databases { get; }
		string CurrentDatabase { get; }
		IStatisticsSet Statistics { get; }
		bool IsInitialized { get; }
		IEnumerable<ServerError> Errors { get; }
		string CurrentDatabaseAddress { get; }
		IAsyncDocumentSession OpenSession();
		void OpenDatabase(string name, Action callback);
		void Connect(Uri serverAddress, Action callback);
		event EventHandler CurrentDatabaseChanged;
		void CreateDatabase(string databaseName, Action callback);
	}
}