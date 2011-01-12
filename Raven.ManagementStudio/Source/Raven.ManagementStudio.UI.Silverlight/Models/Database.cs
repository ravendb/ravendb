namespace Raven.ManagementStudio.UI.Silverlight.Models
{
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using Caliburn.Micro;
	using Client;
	using Client.Document;
	using Plugin;

	public class Database : PropertyChangedBase, IDatabase
	{
		bool _isBusy;

		public Database(string databaseAdress, string databaseName = null)
		{
			this.Address = databaseAdress;
			this.Name = databaseName ?? databaseAdress;
			this.InitializeSession();
		}

		[ImportMany(AllowRecomposition = true)]
		public IList<IPlugin> Plugins { get; set; }

		public bool IsBusy
		{
			get { return _isBusy; }
			set
			{
				_isBusy = value;
				NotifyOfPropertyChange(() => IsBusy);
			}
		}

		#region IDatabase Members

		public IAsyncAttachmentSession AttachmentSession { get; set; }

		public IAsyncCollectionSession CollectionSession { get; set; }

		public IAsyncIndexSession IndexSession { get; set; }

		public IAsyncStatisticsSession StatisticsSession { get; set; }
		public IAsyncDocumentSession Session { get; set; }
		public string Address { get; set; }

		public string Name { get; set; }

		#endregion

		void InitializeSession()
		{
			var store = new DocumentStore
			            	{
			            		Url = this.Address
			            	};

			store.Initialize();

			this.Session = store.OpenAsyncSession();
			this.AttachmentSession = new AsyncAttachmentSession(this.Address);
			this.CollectionSession = new AsyncCollectionSession(this.Address);
			this.IndexSession = new AsyncIndexSession(this.Address);
			this.StatisticsSession = new AsyncStatisticsSession(this.Address);

			this.AttachmentSession = new AsyncAttachmentSession(this.Address);
		}
	}
}