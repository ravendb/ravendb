namespace Raven.Studio.Plugins
{
	using System;
	using System.Collections.Generic;
	using Client;
	using Raven.Database.Data;
	using Statistics;

	public interface IServer
	{
		/// <summary>
		/// The address (url) of the server.
		/// </summary>
		string Address { get; }

		/// <summary>
		/// A list of the names of the databases that are available on the server.
		/// </summary>
		IEnumerable<string> Databases { get; }

		/// <summary>
		/// The name of the currently selected database on the server.
		/// </summary>
		string CurrentDatabase { get; }

		/// <summary>
		/// The set of statistics for the currentlly selected database.
		/// </summary>
		IStatisticsSet Statistics { get; }

		/// <summary>
		/// Recently reported errors from the server.
		/// </summary>
		IEnumerable<ServerError> Errors { get; }

		/// <summary>
		/// The base address that is used from performing operations against the currently selected database.
		/// </summary>
		string CurrentDatabaseAddress { get; }

		/// <summary>
		/// Opens a session against the currently selected database.
		/// </summary>
		/// <returns>An instance of a document session optimized for asynchronous operations.</returns>
		IAsyncDocumentSession OpenSession();

		/// <summary>
		/// Opens a database identified by the name provided. The database becomes the currently selected database.
		/// </summary>
		/// <param name="name">The name of the database to open.</param>
		/// <param name="callback">An action to be performed after the database is opened.</param>
		void OpenDatabase(string name, Action callback);

		/// <summary>
		/// Attempts to connect to the server at the address specified.
		/// </summary>
		/// <param name="serverAddress">The uri of the server to connect to.</param>
		/// <param name="callback">An action to be performed after the server is connected to.</param>
		void Connect(Uri serverAddress, Action callback);

		/// <summary>
		/// An event that is raised whenever the currently selected database is changed.
		/// </summary>
		event EventHandler CurrentDatabaseChanged;

		/// <summary>
		/// Creates a new database on the currently connected server.
		/// </summary>
		/// <param name="databaseName">The name of the database to be created.</param>
		/// <param name="callback">An action to be performed after the database has been created.</param>
		void CreateDatabase(string databaseName, Action callback);
	}
}