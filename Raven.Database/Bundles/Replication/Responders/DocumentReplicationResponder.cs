//-----------------------------------------------------------------------
// <copyright file="DocumentReplicationResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Linq;
using System.Security.Cryptography;
using System.Text;
using NLog;
using Raven.Database.Server;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Data;
using Raven.Bundles.Replication.Plugins;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;
using Raven.Database.Server.Responders;
using Raven.Database.Storage;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Responders
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(AbstractRequestResponder))]
	public class DocumentReplicationResponder : RequestResponder
	{
		private readonly Logger log = LogManager.GetCurrentClassLogger();

		[ImportMany]
		public IEnumerable<AbstractDocumentReplicationConflictResolver> ReplicationConflictResolvers { get; set; }

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
			var array = context.ReadJsonArray();
			using(Database.DisableAllTriggersForCurrentThread())
			{
				Database.TransactionalStorage.Batch(actions =>
				{
					string lastEtag = Guid.Empty.ToString();
					foreach (RavenJObject document in array)
					{
						var metadata = document.Value<RavenJObject>("@metadata");
						if(metadata[ReplicationConstants.RavenReplicationSource] == null)
						{
							// not sure why, old document from when the user didn't have replciation
							// that we suddenly decided to replicate, choose the source for that
							metadata[ReplicationConstants.RavenReplicationSource] = RavenJToken.FromObject(src);
						}
						lastEtag = metadata.Value<string>("@etag");
						var id = metadata.Value<string>("@id");
						document.Remove("@metadata");
						ReplicateDocument(actions, id, metadata, document, src);
					}

					var replicationDocKey = ReplicationConstants.RavenReplicationSourcesBasePath + "/" + src;
					var replicationDocument = Database.Get(replicationDocKey, null);
					var lastAttachmentId = Guid.Empty;
					if (replicationDocument != null)
					{
						lastAttachmentId =
							replicationDocument.DataAsJson.JsonDeserialization<SourceReplicationInformation>().
								LastAttachmentEtag;
					}
					Database.Put(replicationDocKey, null,
								 RavenJObject.FromObject(new SourceReplicationInformation
								 {
									 LastDocumentEtag = new Guid(lastEtag),
									 LastAttachmentEtag = lastAttachmentId,
									 ServerInstanceId = Database.TransactionalStorage.Id
								 }),
								 new RavenJObject(), null);
				});
			}
		}

		private void ReplicateDocument(IStorageActionsAccessor actions, string id, RavenJObject metadata, RavenJObject document, string src)
		{
			var existingDoc = actions.Documents.DocumentByKey(id, null);
			if (existingDoc == null)
			{
				log.Debug("New document {0} replicated successfully from {1}", id, src);
				actions.Documents.AddDocument(id, Guid.Empty, document, metadata);
				return;
			}

			// we just got the same version from the same source - request playback again?
			// at any rate, not an error, moving on
			if (existingDoc.Metadata.Value<string>(ReplicationConstants.RavenReplicationSource) == metadata.Value<string>(ReplicationConstants.RavenReplicationSource)
				&& existingDoc.Metadata.Value<int>(ReplicationConstants.RavenReplicationVersion) == metadata.Value<int>(ReplicationConstants.RavenReplicationVersion))
			{
				return;
			}
			
			
			var existingDocumentIsInConflict = existingDoc.Metadata[ReplicationConstants.RavenReplicationConflict] != null;
			if (existingDocumentIsInConflict == false &&                    // if the current document is not in conflict, we can continue without having to keep conflict semantics
				(IsDirectChildOfCurrentDocument(existingDoc, metadata))) // this update is direct child of the existing doc, so we are fine with overwriting this
			{
				log.Debug("Existing document {0} replicated successfully from {1}", id, src);
				actions.Documents.AddDocument(id, null, document, metadata);
				return;
			}

			if (ReplicationConflictResolvers.Any(replicationConflictResolver => replicationConflictResolver.TryResolve(id, metadata, document, existingDoc)))
			{
				actions.Documents.AddDocument(id, null, document, metadata);
				return;
			}

			metadata[ReplicationConstants.RavenReplicationConflictDocument] = true;
			var newDocumentConflictId = id + "/conflicts/" + HashReplicationIdentifier(metadata);
			metadata.Add(ReplicationConstants.RavenReplicationConflict, RavenJToken.FromObject(true));
			actions.Documents.AddDocument(newDocumentConflictId, null, document, metadata);

			if (existingDocumentIsInConflict) // the existing document is in conflict
			{
				log.Debug("Conflicted document {0} has a new version from {1}, adding to conflicted documents", id, src);
				
				// just update the current doc with the new conflict document
				existingDoc.DataAsJson.Value<RavenJArray>("Conflicts").Add(RavenJToken.FromObject(newDocumentConflictId));
				actions.Documents.AddDocument(id, existingDoc.Etag, existingDoc.DataAsJson, existingDoc.Metadata);
				return;
			}
			log.Debug("Existing document {0} is in conflict with replicated version from {1}, marking document as conflicted", id, src);
				
			// we have a new conflict
			// move the existing doc to a conflict and create a conflict document
			var existingDocumentConflictId = id + "/conflicts/" + HashReplicationIdentifier(existingDoc.Etag ?? Guid.Empty);
			
			existingDoc.Metadata.Add(ReplicationConstants.RavenReplicationConflict, RavenJToken.FromObject(true));
			actions.Documents.AddDocument(existingDocumentConflictId, null, existingDoc.DataAsJson, existingDoc.Metadata);
			actions.Documents.AddDocument(id, null,
			                              new RavenJObject
			                              {
			                              	{
			                              	"Conflicts", new RavenJArray(existingDocumentConflictId, newDocumentConflictId)
			                              	}
			                              },
			                              new RavenJObject
			                              {
			                              	{ReplicationConstants.RavenReplicationConflict, true},
			                              	{"@Http-Status-Code", 409},
			                              	{"@Http-Status-Description", "Conflict"}
			                              });
		}

		private static string HashReplicationIdentifier(RavenJObject metadata)
		{
			using(var md5 = MD5.Create())
			{
				var bytes = Encoding.UTF8.GetBytes(metadata.Value<string>(ReplicationConstants.RavenReplicationSource) + "/" + metadata.Value<string>("@etag"));
				return new Guid(md5.ComputeHash(bytes)).ToString();
			}
		}

		private  string HashReplicationIdentifier(Guid existingEtag)
		{
			using (var md5 = MD5.Create())
			{
				var bytes = Encoding.UTF8.GetBytes(Database.TransactionalStorage.Id + "/" + existingEtag);
				return new Guid(md5.ComputeHash(bytes)).ToString();
			}
		}

		private static bool IsDirectChildOfCurrentDocument(JsonDocument existingDoc, RavenJObject metadata)
		{
			var version = new RavenJObject
			{
				{ReplicationConstants.RavenReplicationSource, existingDoc.Metadata[ReplicationConstants.RavenReplicationSource]},
				{ReplicationConstants.RavenReplicationVersion, existingDoc.Metadata[ReplicationConstants.RavenReplicationVersion]},
			};

			var history = metadata[ReplicationConstants.RavenReplicationHistory];
			if (history == null || history.Type == JTokenType.Null) // no history, not a parent
				return false;

			if (history.Type != JTokenType.Array)
				return false;

			return history.Values().Contains(version, new RavenJTokenEqualityComparer());
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
