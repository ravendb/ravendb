// -----------------------------------------------------------------------
//  <copyright file="DataDumper.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Newtonsoft.Json;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Indexing;
using Raven.Abstractions.Smuggler;
using Raven.Json.Linq;

namespace Raven.Database.Smuggler
{
    public class DataDumper : SmugglerApiBase
    {
        public DataDumper(DocumentDatabase database)
        {
            _database = database;
        }

        private readonly DocumentDatabase _database;

        protected override void EnsureDatabaseExists()
        {
            ensuredDatabaseExists = true;
        }

        protected override void ExportAttachments(JsonTextWriter jsonWriter)
        {
            var totalCount = 0;
            while (true)
            {
                var array = ((Func<int, RavenJArray>) GetAttachments)(totalCount);
                if (array.Length == 0)
                {
                    break;
                }
                totalCount += array.Length;
                foreach (var item in array)
                {
                    item.WriteTo(jsonWriter);
                }
            }
        }

        protected override void FlushBatch(List<RavenJObject> batch)
        {
            _database.Batch(batch.Select(x =>
                                             {
                                                 var metadata = x.Value<RavenJObject>("@metadata");
                                                 var key = metadata.Value<string>("@id");
                                                 x.Remove("@metadata");
                                                 return new PutCommandData
                                                            {
                                                                Document = x,
                                                                Etag = null,
                                                                Key = key,
                                                                Metadata = metadata
                                                            };
                                             }).ToArray());

            batch.Clear();
        }

        protected override RavenJArray GetDocuments(SmugglerOptions options, Guid lastEtag)
        {
            const int dummy = 0;
            return _database.GetDocuments(dummy, 128, lastEtag);
        }

        protected override RavenJArray GetIndexes(int totalCount)
        {
            return _database.GetIndexes(totalCount, 128);
        }

        protected override void PutAttachment(AttachmentExportInfo attachmentExportInfo)
        {
            // Comment taken from Raven.Client.Embedded.EmbeddedDatabaseCommands.PutAttachment(string key, Guid? etag, Stream data, RavenJObject metadata), build 888.
            // we filter out content length, because getting it wrong will cause errors 
            // in the server side when serving the wrong value for this header.
            // worse, if we are using http compression, this value is known to be wrong
            // instead, we rely on the actual size of the data provided for us
            attachmentExportInfo.Metadata.Remove("Content-Length");
            _database.PutStatic(attachmentExportInfo.Key, null, new MemoryStream(attachmentExportInfo.Data), attachmentExportInfo.Metadata);
        }

        protected override void PutIndex(string indexName, RavenJToken index)
        {
            _database.PutIndex(indexName, index.Value<RavenJObject>("definition").JsonDeserialization<IndexDefinition>());
        }

        protected override void ShowProgress(string format, params object[] args)
        {
            if (Progress != null)
            {
                Progress(string.Format(format, args));
            }
        }

        private RavenJArray GetAttachments(int start)
        {
            var array = new RavenJArray();
            var attachmentInfos = _database.GetAttachments(start, 128, null);

            foreach (var attachmentInfo in attachmentInfos)
            {
                // SK: Taken from Raven.Client.Embedded.EmbeddedDatabaseCommands.GetAttachment(string key), build 888.
                var attachment = _database.GetStatic(attachmentInfo.Key);
                if (attachment == null)
                    return null;
                var data = attachment.Data;
                attachment.Data = () =>
                                      {
                                          var memoryStream = new MemoryStream();
                                          _database.TransactionalStorage.Batch(accessor => data().CopyTo(memoryStream));
                                          memoryStream.Position = 0;
                                          return memoryStream;
                                      };
                //var attachment = _store.DatabaseCommands.GetAttachment(attachmentInfo.Key);


                var bytes = StreamToBytes(attachment.Data());

                // Based on Raven.Smuggler.Api.ExportAttachments from build 888.
                array.Add(
                    new RavenJObject
                        {
                            {"Data", bytes},
                            {"Metadata", attachmentInfo.Metadata},
                            {"Key", attachmentInfo.Key}
                        });
            }
            return array;
        }

        /// <summary>
        ///   http://stackoverflow.com/a/221941/2608
        /// </summary>
        private byte[] StreamToBytes(Stream input)
        {
            var buffer = new byte[16*1024];
            using (var ms = new MemoryStream())
            {
                int read;
                while ((read = input.Read(buffer, 0, buffer.Length)) > 0)
                {
                    ms.Write(buffer, 0, read);
                }
                return ms.ToArray();
            }
        }

        public Action<string> Progress { get; set; }
    }
}