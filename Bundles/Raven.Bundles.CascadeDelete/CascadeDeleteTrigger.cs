using Raven.Database;
using Raven.Database.Plugins;
using Raven.Http;
using Newtonsoft.Json.Linq;

namespace Raven.Bundles.CascadeDelete
{
    public class CascadeDeleteTrigger : AbstractDeleteTrigger
    {
        public override void OnDelete(string key, TransactionInformation transactionInformation)
        {
            if (CascadeDeleteContext.IsInCascadeDeleteContext)
                return;

            var document = Database.Get(key, transactionInformation);
            if (document == null)
                return;

            using (CascadeDeleteContext.Enter())
            {
                var documentsToDelete = document.Metadata.Value<JArray>(MetadataKeys.DocumentsToCascadeDelete);

                if (documentsToDelete != null)
                {
                    foreach (var documentToDelete in documentsToDelete)
                    {
                        var documentId = documentToDelete.Value<string>();
                        if (!CascadeDeleteContext.HasAlreadyDeletedDocument(documentId))
                        {
                            CascadeDeleteContext.AddDeletedDocument(documentId);
                            Database.Delete(documentId, null, transactionInformation);
                        }
                    }
                }

                var attachmentsToDelete = document.Metadata.Value<JArray>(MetadataKeys.AttachmentsToCascadeDelete);

                if (attachmentsToDelete != null)
                    foreach (var attachmentToDelete in attachmentsToDelete)
                        Database.DeleteStatic(attachmentToDelete.Value<string>(), null);
            }
        }
    }
}
