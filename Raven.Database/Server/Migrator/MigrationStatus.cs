using System;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler.Data;

namespace Raven.Database.Server.Migrator
{
    public class MigrationStatus : OperationStateBase
    {
        public MigrationState MigrationState;

        public void MarkCompleted(string initialMessage, TimeSpan elapsed, OperationState opertaionState)
        {
            MarkCompleted(initialMessage, elapsed);

            MigrationState.LastDocumentEtag = opertaionState.LastDocsEtag;
            MigrationState.LastDocumentDeleteEtag = opertaionState.LastDocDeleteEtag;
            MigrationState.LastAttachmentEtag = opertaionState.LastAttachmentsEtag;
            MigrationState.LastAttachmentDeleteEtag = opertaionState.LastAttachmentsDeleteEtag;
        }
    }
}