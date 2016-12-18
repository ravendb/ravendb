// -----------------------------------------------------------------------
//  <copyright file="VersioningMetadataUpdateTrigger.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.ComponentModel.Composition;
using System.IO;
using System.Web.UI;
using Raven.Database.FileSystem.Plugins;
using Raven.Database.FileSystem.Storage;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Bundles.Versioning.Plugins
{
    [InheritedExport(typeof(AbstractFileMetadataUpdateTrigger))]
    [ExportMetadata("Bundle", "Versioning")]
    public class VersioningMetadataUpdateTrigger : AbstractFileMetadataUpdateTrigger
    {
        private VersioningTriggerActions actions;

        public override void Initialize()
        {
            actions = new VersioningTriggerActions(FileSystem);
        }

        public override VetoResult AllowUpdate(string name, RavenJObject metadata)
        {
            return actions.AllowOperation(name, metadata);
        }

        public override void OnUpdate(string name, RavenJObject metadata)
        {
            actions.InitializeMetadata(name, metadata);
        }

        public override void AfterUpdate(string name, RavenJObject metadata)
        {
            var revisionFile = actions.PutRevisionFile(name, null, metadata);

            if(revisionFile == null)
                return;

            FileSystem.Storage.Batch(accessor =>
            {
                var start = 0;
                const int pagesToLoad = 1024;

                FileAndPagesInformation fileWithPages;

                do
                {
                    fileWithPages = accessor.GetFile(name, start, pagesToLoad);

                    foreach (var page in fileWithPages.Pages)
                    {
                        // update the usage count of the page
                        accessor.AssociatePage(revisionFile, page.Id, page.PositionInFile, page.Size, incrementUsageCount: true);
                    }

                    start += pagesToLoad;

                } while (fileWithPages.Pages.Count == pagesToLoad);

                accessor.CompleteFileUpload(revisionFile);
            });
        }
    }
}
