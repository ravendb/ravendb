// -----------------------------------------------------------------------
//  <copyright file="AdminRevisionsHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.Handlers.Admin.Processors.Revisions;
using Raven.Server.Documents.Handlers.Processors.Revisions;
using Raven.Server.Routing;

namespace Raven.Server.Documents.Handlers.Admin
{
    public sealed class AdminRevisionsHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/revisions", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task DeleteRevisionsFor()
        {
            using (var processor = new AdminRevisionsHandlerProcessorForDeleteRevisions(this))
                await processor.ExecuteAsync();
        }

        public const string ConflictedRevisionsConfigTag = "conflicted-revisions-config";

        [RavenAction("/databases/*/admin/revisions/conflicts/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigConflictedRevisions()
        {
            using (var processor = new AdminRevisionsHandlerProcessorForPostRevisionsConflictsConfiguration(this))
                await processor.ExecuteAsync();
        }

        public const string ReadRevisionsConfigTag = "read-revisions-config";

        [RavenAction("/databases/*/admin/revisions/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task PostRevisionsConfiguration()
        {
            using (var processor = new RevisionsHandlerProcessorForPostRevisionsConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/revisions/config/enforce", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task EnforceConfigRevisions()
        {
            using (var processor = new AdminRevisionsHandlerProcessorForEnforceRevisionsConfiguration(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/revisions/orphaned/adopt", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task AdoptOrphans()
        {
            using (var processor = new AdminRevisionsHandlerProcessorForAdoptOrphanedRevisions(this))
                await processor.ExecuteAsync();
        }
    }
}
