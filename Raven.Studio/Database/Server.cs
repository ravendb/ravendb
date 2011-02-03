namespace Raven.Studio.Database
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Client;
	using Client.Document;
	using Framework;
	using Plugin;

	public class Server : PropertyChangedBase, IServer
	{
		readonly DocumentStore store;
		bool isInitialized;

		public Server(string address, string name = null)
		{
			Address = address;
			Name = name ?? address;

			store = new DocumentStore {Url = Address};
			store.Initialize();

			store.OpenAsyncSession().Advanced.AsyncDatabaseCommands
				.GetDatabaseNamesAsync()
				.ContinueOnSuccess(t => 
				{
					var databases = new List<string>{"Default"};
					databases.AddRange(t.Result);
					Databases = databases;

					CurrentDatabase = databases[0];

					IsInitialized = true;
				});
		}

		string currentDatabase;
		public string CurrentDatabase
		{
			get { return currentDatabase; }
			set { currentDatabase = value; NotifyOfPropertyChange( ()=> CurrentDatabase); }
		}

		[ImportMany(AllowRecomposition = true)]
		public IList<IPlugin> Plugins { get; set; }

		IEnumerable<string> databases;
		public IEnumerable<string> Databases
		{
			get { return databases; }
			private set { databases = value; NotifyOfPropertyChange( ()=> Databases); }
		}

		public bool IsInitialized
		{
			get { return isInitialized; }
			set
			{
				isInitialized = value;
				NotifyOfPropertyChange(() => IsInitialized);
			}
		}
		public string Address { get; private set; }
		public string Name { get; private set; }

		public IAsyncDocumentSession OpenSession()
		{
			return (CurrentDatabase == "Default") 
				? store.OpenAsyncSession()
				: store.OpenAsyncSession(CurrentDatabase);
		}
	}
}