using System;
using System.Collections.Generic;
using System.Net;
using System.Reactive.Linq;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Documents;
using System.Windows.Ink;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Animation;
using System.Windows.Shapes;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Database.Util;
using Raven.Studio.Commands;
using Raven.Studio.Extensions;
using Raven.Studio.Features.Documents;
using Raven.Studio.Infrastructure;
using Raven.Abstractions.Extensions;
using System.Linq;

namespace Raven.Studio.Models
{
    public class ConflictsModel : PageViewModel
    {
        private static readonly string ConflictsIndexName = "Raven/ConflictDocuments";
        private IDisposable changesSubscription;
        private ICommand deleteSelectedDocuments;
        private ICommand copyIdsToClipboard;
        private ICommand editDocument;
        private Dictionary<string, ReplicationSourceInfo> replicationSourcesLookup;
        private static ConcurrentSet<string> performedIndexChecks = new ConcurrentSet<string>();
        private IDisposable replicationSourcesChanges;

        public VirtualCollection<ViewableDocument> ConflictDocuments { get; private set; }

        public ItemSelection<VirtualItem<ViewableDocument>> ItemSelection { get; private set; }

        public Dictionary<string, ReplicationSourceInfo> ReplicationSourcesLookup
        {
            get { return replicationSourcesLookup; }
            private set
            {
                replicationSourcesLookup = value;
                OnPropertyChanged(() => ReplicationSourcesLookup);
            }
        }

        public ConflictsModel()
        {
            ConflictDocuments = new VirtualCollection<ViewableDocument>(new ConflictDocumentsCollectionSource(), 30, 30);
            ItemSelection = new ItemSelection<VirtualItem<ViewableDocument>>();
        }

        public ICommand DeleteSelectedDocuments
        {
            get { return deleteSelectedDocuments ?? (deleteSelectedDocuments = new DeleteDocumentsCommand(ItemSelection)); }
        }

        public ICommand CopyIdsToClipboard
        {
            get { return copyIdsToClipboard ?? (copyIdsToClipboard = new CopyDocumentsIdsCommand(ItemSelection)); }
        }

        public ICommand EditDocument
        {
            get
            {
                return editDocument ??
                       (editDocument =
                        new EditVirtualDocumentCommand() { DocumentNavigatorFactory  = (id, itemIndex) => new ConflictDocumentsNavigator(id, itemIndex)});
            }
        }

        protected override void OnViewLoaded()
        {
            ApplicationModel.Database
                .ObservePropertyChanged()
                .TakeUntil(Unloaded)
                .Subscribe(_ =>
                {
                    EnsureIndexExists();
                    ObserveSourceChanges();
                    ConflictDocuments.Refresh(RefreshMode.ClearStaleData);
                });

            ObserveSourceChanges();
            ConflictDocuments.Refresh(RefreshMode.ClearStaleData);
            LoadReplicationSources();
            EnsureIndexExists();
        }

        private void LoadReplicationSources()
        {
            ApplicationModel.DatabaseCommands.StartsWithAsync("Raven/Replication/Sources", null, 0, 1024)
                            .ContinueOnSuccessInTheUIThread(
                                docs =>
                                {
                                    var sourcesLookup = docs.ToDictionary(d => d.DataAsJson.Value<string>("ServerInstanceId"), d => new ReplicationSourceInfo(d.DataAsJson.Value<string>("Source")));
                                    var currentUrl = ApplicationModel.Current.Server.Value.Url + "databases/" + ApplicationModel.Database.Value.Name;

                                    sourcesLookup.Add(ApplicationModel.Database.Value.Statistics.Value.DatabaseId.ToString(), new ReplicationSourceInfo(currentUrl));

                                    ReplicationSourcesLookup = sourcesLookup;
                                });
        }

        private void EnsureIndexExists()
        {
            if (performedIndexChecks.Contains(ApplicationModel.Database.Value.Name))
            {
                return;
            }

            if (!performedIndexChecks.TryAdd(ApplicationModel.Database.Value.Name))
            {
                return;
            }

            CreateIndex();
        }

        private void CreateIndex()
        {
            var index = new IndexDefinition()
            {
                Map = @"from doc in docs
                        let id = doc[""@metadata""][""@id""]
                        where doc[""@metadata""][""Raven-Replication-Conflict""] == true && (id.Length < 47 || !id.Substring(id.Length - 47).StartsWith(""/conflicts/"", StringComparison.OrdinalIgnoreCase))
                        select new { ConflictDetectedAt = (DateTime)doc[""@metadata""][""Last-Modified""]}",
                TransformResults = @"from result in results
                                    select new { 
	                                    Id = result[""@metadata""][""@id""], 
	                                    ConflictDetectedAt = (DateTime)result[""@metadata""][""Last-Modified""], 
	                                    Versions = result.Conflicts.Select(versionId => { var version = Database.Load(versionId); return new { Id = versionId, SourceId = version[""@metadata""][""Raven-Replication-Source""]}; })
                                    }"
            };

            ApplicationModel.DatabaseCommands.PutIndexAsync(ConflictsIndexName, index, true).CatchIgnore();
        }

        protected override void OnViewUnloaded()
        {
            StopListeningForChanges();
        }

        private void ObserveSourceChanges()
        {
            if (!IsLoaded)
                return;

            StopListeningForChanges();

            var databaseModel = ApplicationModel.Database.Value;

            if (databaseModel != null)
            {
                changesSubscription =
                    databaseModel.IndexChanges.Where(i => i.Name.Equals(ConflictsIndexName, StringComparison.Ordinal))
                    .SampleResponsive(TimeSpan.FromSeconds(1))
                    .ObserveOnDispatcher()
                    .Subscribe(_ => ConflictDocuments.Refresh(RefreshMode.PermitStaleDataWhilstRefreshing));

                replicationSourcesChanges =
                    databaseModel.DocumentChanges.Where(
                        d => d.Id.StartsWith("Raven/Replication/Sources/", StringComparison.OrdinalIgnoreCase))
                                 .SampleResponsive(TimeSpan.FromSeconds(1))
                                 .ObserveOnDispatcher()
                                 .Subscribe(_ => LoadReplicationSources());
            }
        }

        private void StopListeningForChanges()
        {
            if (changesSubscription != null)
                changesSubscription.Dispose();

            if (replicationSourcesChanges != null)
                replicationSourcesChanges.Dispose();
        }
    }
}
