//-----------------------------------------------------------------------
// <copyright file="DocumentReplicationResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Database.Storage;
using Raven.Json.Linq;

using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;

namespace Raven.Bundles.Replication.Responders
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(AbstractRequestResponder))]
	public class DocumentReplicationResponder : AbstractRequestResponder
	{
		private readonly ILog log = LogManager.GetCurrentClassLogger();
		private ReplicationTask replicationTask;
		public ReplicationTask ReplicationTask
		{
			get { return replicationTask ?? (replicationTask = Database.StartupTasks.OfType<ReplicationTask>().FirstOrDefault()); }
		}

		[ImportMany]
		public IEnumerable<AbstractDocumentReplicationConflictResolver> ReplicationConflictResolvers { get; set; }


        private readonly string EmptyEtagString = Etag.Empty.ToString();
        private const int PulseThreshold = 512;

		public override void Respond(IHttpContext context)
		{
			var src = context.Request.QueryString["from"];
			if (string.IsNullOrEmpty(src))
			{
				context.SetStatusToBadRequest();
				return;
			}
			while (src.EndsWith("/"))
				src = src.Substring(0, src.Length - 1);// remove last /, because that has special meaning for Raven
			if (string.IsNullOrEmpty(src))
			{
				context.SetStatusToBadRequest();
				return;
			}

		    long bytesRead;
            var array = context.ReadJsonArray(out bytesRead);
		    if (log.IsDebugEnabled)
		    {
                log.Debug(string.Format("Read {0} documents and {1} bytes for replication batch", array.Length, bytesRead));
		    }

			if (ReplicationTask != null)
				ReplicationTask.HandleHeartbeat(src);

			using (Database.DisableAllTriggersForCurrentThread())
			{
                string lastEtag = EmptyEtagString;

                var position = 0;
                while (position < array.Length)
                {
                    // save documents in batches
                    using (Database.DocumentLock.Lock())
                    {
                        Database.TransactionalStorage.Batch(actions =>
                        {
                            var count = 0;
                            while (count < PulseThreshold && position < array.Length)
                            {
                                var document = (RavenJObject)array[position];
                                var metadata = document.Value<RavenJObject>("@metadata");
                                if (metadata[Constants.RavenReplicationSource] == null)
                                {
                                    // not sure why, old document from when the user didn't have replication
                                    // that we suddenly decided to replicate, choose the source for that
                                    metadata[Constants.RavenReplicationSource] = RavenJToken.FromObject(src);
                                }
                                lastEtag = metadata.Value<string>("@etag");
                                var id = metadata.Value<string>("@id");
                                document.Remove("@metadata");

                                ReplicateDocument(actions, id, metadata, document, src);
                                count++;
                                position++;
                            }
                        });
                    }
                }

                using (Database.DocumentLock.Lock())
                {
                    // save the last etag
                    Database.TransactionalStorage.Batch(actions =>
                    {
                        SaveReplicationSource(context, src, lastEtag);
                    });
                }
			}
		}

		private void SaveReplicationSource(IHttpContext context, string src, string lastEtag)
		{
			var replicationDocKey = Constants.RavenReplicationSourcesBasePath + "/" + src;
			var replicationDocument = Database.Get(replicationDocKey, null);
			var lastAttachmentId = Etag.Empty;
			if (replicationDocument != null)
				lastAttachmentId = replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().LastAttachmentEtag;

			Guid serverInstanceId;
			if (Guid.TryParse(context.Request.QueryString["dbid"], out serverInstanceId) == false)
				serverInstanceId = Database.TransactionalStorage.Id;

			Database.Put(
				replicationDocKey,
				null,
				RavenJObject.FromObject(
					new SourceReplicationInformation
					{
						Source = src,
						LastDocumentEtag = Etag.Parse(lastEtag),
						LastAttachmentEtag = lastAttachmentId,
						ServerInstanceId = serverInstanceId
					}),
				new RavenJObject(),
				null);
		}

		private void ReplicateDocument(IStorageActionsAccessor actions, string id, RavenJObject metadata, RavenJObject document, string src)
		{
			try
			{
				new DocumentReplicationBehavior
				{
					Actions = actions,
					Database = Database,
					ReplicationConflictResolvers = ReplicationConflictResolvers,
					Src = src
				}.Replicate(id, metadata, document);
			}
			catch (Exception ex)
			{
				log.ErrorException(
					string.Format("Exception occurred during the replication of the document {0} from the server {1}", id, src), ex);
				throw;
			}
		}

		public override string UrlPattern
		{
			get { return "^/replication/replicateDocs$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "POST" }; }
		}
	}
}
