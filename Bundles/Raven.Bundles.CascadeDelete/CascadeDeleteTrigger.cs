using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;

namespace Raven.Bundles.CascadeDelete
{
    public class CascadeDeleteTrigger : AbstractDeleteTrigger
    {
        public override VetoResult AllowDelete(string key, TransactionInformation transactionInformation)
        {
            return VetoResult.Allowed;
        }

        public override void OnDelete(string key, TransactionInformation transactionInformation)
        {
            var document = Database.Get(key, null);
            if (document == null)
                return;

            var documentsToDelete = document.Metadata.Value<string[]>(MetadataKeys.DocumentsToCascadeDelete);

            if (documentsToDelete != null)
            {
                foreach (var documentToDelete in documentsToDelete)
                {
                    Database.Delete(documentToDelete, null, null);
                }
            }

            var attachmentsToDelete = document.Metadata.Value<string[]>(MetadataKeys.AttachmentsToCascadeDelete);

            if (attachmentsToDelete != null)
            {
                foreach (var attachmentToDelete in attachmentsToDelete)
                {
                    Database.DeleteStatic(attachmentToDelete,null);
                }
            }
        }

        public override void AfterCommit(string key)
        {
            // no-op
        }

    }
}
