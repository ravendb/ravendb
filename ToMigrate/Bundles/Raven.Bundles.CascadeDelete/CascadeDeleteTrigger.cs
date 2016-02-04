//-----------------------------------------------------------------------
// <copyright file="CascadeDeleteTrigger.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Database.Plugins;
using Raven.Json.Linq;

namespace Raven.Bundles.CascadeDelete
{
    public class CascadeDeleteTrigger : AbstractDeleteTrigger
    {
        public override void OnDelete(string key)
        {
            if (CascadeDeleteContext.IsInCascadeDeleteContext)
                return;

            using (Database.DisableAllTriggersForCurrentThread())
            using (CascadeDeleteContext.Enter())
            {
                RecursiveDelete(key);
            }
        }

        private void RecursiveDelete(string key)
        {
            var document = Database.Documents.Get(key);
            if (document == null)
                return;

            var documentsToDelete = document.Metadata.Value<RavenJArray>(MetadataKeys.DocumentsToCascadeDelete);
            if (documentsToDelete != null)
            {
                foreach (var documentToDelete in documentsToDelete)
                {
                    var documentId = documentToDelete.Value<string>();
                    if (!CascadeDeleteContext.HasAlreadyDeletedDocument(documentId))
                    {
                        CascadeDeleteContext.AddDeletedDocument(documentId);
                        RecursiveDelete(documentId);
                        Database.Documents.Delete(documentId, null);
                    }
                }
            }

            return;
        }
    }
}
