namespace Raven.Studio.Features.Database
{
	using System;
	using System.Collections.Generic;
	using Client;
	using Statistics;

	public interface IServer
	{
		string Address { get; }
		string Name { get; }
		IEnumerable<string> Databases { get; }
		string CurrentDatabase { get; }
		IStatisticsSet Statistics { get; }
		bool IsInitialized { get; }
		IAsyncDocumentSession OpenSession();
		void OpenDatabase(string name, Action callback);
		void Connect(Uri serverAddress, Action callback);
		event EventHandler CurrentDatabaseChanged;
		event EventHandler Connected;
	}
}