// -----------------------------------------------------------------------
//  <copyright file="ExportOptions.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Smuggler.Data;

namespace Raven.Abstractions.Smuggler
{
    public class ExportOptions
    {
        private ExportOptions()
        {
            StartEtags = new LastEtagsInfo
            {
                LastAttachmentsDeleteEtag = null,
                LastDocsEtag = null,
                LastDocDeleteEtag = null,
                LastAttachmentsEtag = null
            };

            MaxNumberOfAttachmentsToExport = int.MaxValue;
            MaxNumberOfDocumentsToExport = int.MaxValue;
        }

        public bool ExportDocuments { get; set; }

        public bool ExportAttachments { get; set; }

        public bool ExportDeletions { get; set; }

        public LastEtagsInfo StartEtags { get; set; }

        private int maxNumberOfDocumentsToExport;
        public int MaxNumberOfDocumentsToExport
        {
            get
            {
                return maxNumberOfDocumentsToExport;
            }

            set
            {
                if (value < 0)
                {
                    maxNumberOfDocumentsToExport = 0;
                    return;
                }

                maxNumberOfDocumentsToExport = value;
            }
        }

        private int maxNumberOfAttachmentsToExport;
        public int MaxNumberOfAttachmentsToExport
        {
            get
            {
                return maxNumberOfAttachmentsToExport;
            }

            set
            {
                if (value < 0)
                {
                    maxNumberOfAttachmentsToExport = 0;
                    return;
                }

                maxNumberOfAttachmentsToExport = value;
            }
        }

        public static ExportOptions Create(OperationState state, ItemType types, bool exportDeletions, int maxNumberOfItemsToExport)
        {
            return new ExportOptions
            {
                ExportAttachments = types.HasFlag(ItemType.Attachments),
                ExportDocuments = types.HasFlag(ItemType.Documents),
                ExportDeletions = exportDeletions,
                StartEtags = state,
                MaxNumberOfDocumentsToExport = maxNumberOfItemsToExport - state.NumberOfExportedDocuments,
                MaxNumberOfAttachmentsToExport = maxNumberOfItemsToExport - state.NumberOfExportedAttachments
            };
        }
    }
}