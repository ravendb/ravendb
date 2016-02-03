// -----------------------------------------------------------------------
//  <copyright file="SmugglerExporter.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Threading;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Json.Linq;

namespace Raven.Database.Smuggler
{
    internal class SmugglerExporter
    {
        private readonly DocumentDatabase database;

        private readonly ExportOptions options;

        private readonly IDictionary<SmugglerExportType, string> exportTypes = new Dictionary<SmugglerExportType, string>();

        public SmugglerExporter(DocumentDatabase database, ExportOptions options)
        {
            this.database = database;
            this.options = options;

            FillExportTypes();
        }

        public void Export(Action<RavenJObject> write, CancellationToken token)
        {
            database.TransactionalStorage.Batch(accessor =>
            {
                var state = new LastEtagsInfo
                {
                    LastAttachmentsDeleteEtag = options.StartEtags.LastAttachmentsDeleteEtag,
                    LastDocsEtag = options.StartEtags.LastDocsEtag,
                    LastDocDeleteEtag = options.StartEtags.LastDocDeleteEtag,
                    LastAttachmentsEtag = options.StartEtags.LastAttachmentsEtag
                };

                if (options.ExportDocuments)
                    state.LastDocsEtag = WriteDocuments(write, options.StartEtags.LastDocsEtag, options.MaxNumberOfDocumentsToExport, token);

                if (options.ExportAttachments)
                    state.LastAttachmentsEtag = WriteAttachments(write, options.StartEtags.LastAttachmentsEtag, options.MaxNumberOfAttachmentsToExport, token);

                if (options.ExportDeletions)
                {
                    state.LastDocDeleteEtag = WriteDocumentDeletions(write, options.StartEtags.LastDocDeleteEtag, token);
                    state.LastAttachmentsDeleteEtag = WriteAttachmentDeletions(write, options.StartEtags.LastAttachmentsDeleteEtag, token);
                }

                WriteIdentities(write, token);

                write(new RavenJObject
                {
                    { "Type", exportTypes[SmugglerExportType.Summary] },
                    { "Item", RavenJObject.FromObject(state) }
                });
            });
        }

        private void WriteIdentities(Action<RavenJObject> write, CancellationToken token)
        {
            database.TransactionalStorage.Batch(accessor =>
            {
                var start = 0;
                const int PageSize = 1024;

                do
                {
                    token.ThrowIfCancellationRequested();

                    long count = 0;
                    long totalCount;
                    foreach (var identity in accessor.General.GetIdentities(start, PageSize, out totalCount))
                    {
                        count++;

                        write(new RavenJObject
                        {
                            { "Type", exportTypes[SmugglerExportType.Identity] },
                            { "Item", new RavenJObject
                                {
                                    { "Key", identity.Key },
                                    { "Value", identity.Value.ToString() }
                                }
                            }
                        });
                    }

                    if (count == 0)
                        break;

                    start += PageSize;
                } while (true);
            });
        }

        private Etag WriteAttachmentDeletions(Action<RavenJObject> write, Etag startEtag, CancellationToken token)
        {
            var endEtag = startEtag;
            database.TransactionalStorage.Batch(accessor =>
            {
                foreach (var listItem in accessor.Lists.Read(Constants.RavenPeriodicExportsAttachmentsTombstones, startEtag, null, int.MaxValue))
                {
                    token.ThrowIfCancellationRequested();

                    endEtag = listItem.Etag;

                    write(new RavenJObject
                    {
                        { "Type", exportTypes[SmugglerExportType.AttachmentDeletion] },
                        { "Item", new RavenJObject
                            {
                                {"Key", listItem.Key}
                            }
                        }
                    });
                }
            });

            return endEtag;
        }

        private Etag WriteDocumentDeletions(Action<RavenJObject> write, Etag startEtag, CancellationToken token)
        {
            var endEtag = startEtag;
            database.TransactionalStorage.Batch(accessor =>
            {
                foreach (var listItem in accessor.Lists.Read(Constants.RavenPeriodicExportsDocsTombstones, startEtag, null, int.MaxValue))
                {
                    token.ThrowIfCancellationRequested();

                    endEtag = listItem.Etag;

                    write(new RavenJObject
                    {
                        { "Type", exportTypes[SmugglerExportType.DocumentDeletion] },
                        { "Item", new RavenJObject
                            {
                                {"Key", listItem.Key}
                            }
                        }
                    });
                }
            });

            return endEtag;
        }

        private Etag WriteAttachments(Action<RavenJObject> write, Etag startEtag, int maxNumberOfAttachmentsToWrite, CancellationToken token)
        {
            var endEtag = startEtag;
            database.TransactionalStorage.Batch(actions =>
            {
                foreach (var attachmentInformation in actions.Attachments.GetAttachmentsAfter(startEtag, maxNumberOfAttachmentsToWrite, long.MaxValue))
                {
                    token.ThrowIfCancellationRequested();

                    endEtag = attachmentInformation.Etag;

                    var attachment = actions.Attachments.GetAttachment(attachmentInformation.Key);
                    using (var stream = attachment.Data())
                    {
                        write(new RavenJObject
                        {
                            { "Type", exportTypes[SmugglerExportType.Attachment] },
                            { "Item", new RavenJObject
                                {
                                    { "Data", stream.ReadData() },
                                    { "Metadata", attachmentInformation.Metadata },
                                    { "Key", attachmentInformation.Key },
                                    { "Etag", new RavenJValue(attachmentInformation.Etag.ToString()) }
                                }
                            }
                        });
                    }
                }
            });

            return endEtag;
        }

        private Etag WriteDocuments(Action<RavenJObject> write, Etag startEtag, int maxNumberOfDocumentsToWrite, CancellationToken token)
        {
            return database.Documents.GetDocuments(0, maxNumberOfDocumentsToWrite, startEtag, token, document =>
            {
                write(new RavenJObject
                {
                    { "Type", exportTypes[SmugglerExportType.Document] },
                    { "Item", document.ToJson() }
                });

                return true;
            }) ?? startEtag;
        }

        private void FillExportTypes()
        {
            foreach (SmugglerExportType value in Enum.GetValues(typeof(SmugglerExportType)))
                exportTypes.Add(value, value.ToString());
        }

    }
}