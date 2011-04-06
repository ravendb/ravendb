namespace Raven.Studio.Features.Collections
{
    using System;
    using System.ComponentModel.Composition;
    using System.Text;
    using System.Windows.Markup;
    using Abstractions.Data;
    using Caliburn.Micro;
    using Database;
    using Documents;
    using Messages;
    using Newtonsoft.Json.Linq;
    using Plugins;

	[Export]
    public class EditCollectionTemplateViewModel : Screen
    {
        private readonly IEventAggregator events;
        private readonly IServer server;
        private readonly IDocumentTemplateProvider templateProvider;
        private string validationSummary;
        private string xaml;

        [ImportingConstructor]
        public EditCollectionTemplateViewModel(IServer server, IEventAggregator events,
                                               IDocumentTemplateProvider templateProvider)
        {
            this.server = server;
            this.events = events;
            this.templateProvider = templateProvider;
        }

        private JObject Metadata { get; set; }

        public string Xaml
        {
            get { return xaml; }
            set
            {
                xaml = value;
                ValidationSummary = string.Empty;
                NotifyOfPropertyChange(() => Xaml);
            }
        }

        private Guid? Etag { get; set; }

        private string TemplateKey
        {
            get { return Collection.Name + "Template"; }
        }

        public Collection Collection { get; set; }

        public string ValidationSummary
        {
            get { return validationSummary; }
            set
            {
                validationSummary = value;
                NotifyOfPropertyChange(() => ValidationSummary);
            }
        }

        private bool IsXamlValid()
        {
            try
            {
                XamlReader.Load(Xaml);
            }
            catch (Exception e)
            {
                ValidationSummary = e.Message;
                return false;
            }

            return true;
        }

        public void Save()
        {
            if (!IsXamlValid()) return;

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

        private void SetDefaultXaml()
        {
            Xaml = templateProvider.GetDefaultTemplateXamlFor(TemplateKey.Replace("Template", string.Empty));
        }

        private void Reset()
        {
            Etag = null;
            Metadata = null;
            Xaml = string.Empty;
        }
    }
}