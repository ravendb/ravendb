namespace Raven.Studio.Models
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Client;
	using Client.Document;
	using Plugin;

	public class Database : PropertyChangedBase, IDatabase
	{
		bool isBusy;

		public Database(string databaseAdress, string databaseName = null)
		{
			Address = databaseAdress;
			Name = databaseName ?? databaseAdress;
			InitializeSession();
		}

		[ImportMany(AllowRecomposition = true)]
		public IList<IPlugin> Plugins { get; set; }

		public bool IsBusy
		{
			get { return isBusy; }
			set
			{
				isBusy = value;
				NotifyOfPropertyChange(() => IsBusy);
			}
		}

		#region IDatabase Members

		public IAsyncDocumentSession Session { get; set; }
		public string Address { get; set; }
		public string Name { get; set; }

		#endregion

		void InitializeSession()
		{
			var store = new DocumentStore {Url = Address};

			store.Initialize();

			Session = store.OpenAsyncSession();
		}
	}
}