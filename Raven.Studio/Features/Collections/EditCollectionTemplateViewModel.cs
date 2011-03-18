namespace Raven.Studio.Features.Collections
{
	using System;
	using System.ComponentModel.Composition;
	using System.Text;
	using Abstractions.Data;
	using Caliburn.Micro;
	using Database;
	using Documents;
	using Messages;
	using Newtonsoft.Json.Linq;

	[Export]
	public class EditCollectionTemplateViewModel : Screen
	{
		readonly IEventAggregator events;
		readonly IDocumentTemplateProvider templateProvider;
		readonly IServer server;
		string xaml;

		[ImportingConstructor]
		public EditCollectionTemplateViewModel(IServer server, IEventAggregator events, IDocumentTemplateProvider templateProvider)
		{
			this.server = server;
			this.events = events;
			this.templateProvider = templateProvider;
		}

		JObject Metadata { get; set; }

		public string Xaml
		{
			get { return xaml; }
			set
			{
				xaml = value;
				NotifyOfPropertyChange(() => Xaml);
			}
		}

		Guid? Etag { get; set; }

		string TemplateKey
		{
			get { return Collection.Name + "Template"; }
		}

		public Collection Collection { get; set; }

		public void Save()
		{
			//TODO: validate xaml before saving

			using (var session = server.OpenSession())
			{
				var encoding = new UTF8Encoding();
				var bytes = encoding.GetBytes(Xaml);

				session.Advanced.AsyncDatabaseCommands
					.PutAttachmentAsync(TemplateKey, Etag, bytes, Metadata)
					.ContinueWith(put => events.Publish(new CollectionTemplateUpdated(TemplateKey, Xaml)));
			}
		}

		public void RestoreDefault()
		{
			SetDefaultXaml();

			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.DeleteAttachmentAsync(TemplateKey, Etag)
					.ContinueWith(put => events.Publish(new CollectionTemplateUpdated(TemplateKey, null)));
			}
		}

		protected override void OnActivate()
		{
			Reset();

			using (var session = server.OpenSession())
			{
				session.Advanced.AsyncDatabaseCommands
					.GetAttachmentAsync(TemplateKey)
					.ContinueWith(get =>
					              	{
					              		if (get.Result == null)
					              		{
					              			SetDefaultXaml();
											return;
					              		}

					              		Etag = get.Result.Etag;
					              		Metadata = get.Result.Metadata;

					              		var encoding = new UTF8Encoding();
					              		var bytes = get.Result.Data;
					              		Xaml = encoding.GetString(bytes, 0, bytes.Length);
					              	});
			}
		}

		void SetDefaultXaml()
		{
			Xaml = templateProvider.GetTemplateXamlFor(TemplateKey.Replace("Template",string.Empty));
		}

		void Reset()
		{
			Etag = null;
			Metadata = null;
			Xaml = string.Empty;
		}
	}
}