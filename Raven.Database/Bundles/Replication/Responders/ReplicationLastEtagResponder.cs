//-----------------------------------------------------------------------
// <copyright file="ReplicationLastEtagResponder.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Bundles.Replication.Data;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Responders
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(AbstractRequestResponder))]
	public class ReplicationLastEtagResponder : AbstractRequestResponder
	{
		private ILog log = LogManager.GetCurrentClassLogger();

		public override void Respond(IHttpContext context)
		{
			var src = context.Request.QueryString["from"];
			var dbid = context.Request.QueryString["dbid"];
			if (dbid == Database.TransactionalStorage.Id.ToString())
				throw new InvalidOperationException("Both source and target databases have database id = " + dbid + "\r\nDatabase cannot replicate to itself.");

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

			switch (context.Request.HttpMethod)
			{
				case "GET":
					OnGet(context, src);
					break;
				case "PUT":
					OnPut(context, src);
					break;	

			}
		}

		private void OnGet(IHttpContext context, string src)
		{
			using (Database.DisableAllTriggersForCurrentThread())
			{
				var document = Database.Get(Constants.RavenReplicationSourcesBasePath + "/" + src, null);

				SourceReplicationInformation sourceReplicationInformation;

				Guid serverInstanceId = Database.TransactionalStorage.Id; // this is my id, sent to the remote serve

				if (document == null)
				{
					sourceReplicationInformation = new SourceReplicationInformation()
					{
						Source = src,
						ServerInstanceId = serverInstanceId
					};
				}
				else
				{
					sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
					sourceReplicationInformation.ServerInstanceId = serverInstanceId;
				}

				var currentEtag = context.Request.QueryString["currentEtag"];
				log.Debug("Got replication last etag request from {0}: [Local: {1} Remote: {2}]", src,
						  sourceReplicationInformation.LastDocumentEtag, currentEtag);
				context.WriteJson(sourceReplicationInformation);
			}
		}

		private void OnPut(IHttpContext context, string src)
		{
			using (Database.DisableAllTriggersForCurrentThread())
			{
				var document = Database.Get(Constants.RavenReplicationSourcesBasePath + "/" + src, null);

				SourceReplicationInformation sourceReplicationInformation;

				Etag docEtag = null, attachmentEtag = null;
				try
				{
					docEtag = Etag.Parse(context.Request.QueryString["docEtag"]);
				}
				catch
				{

				}
				try
				{
					attachmentEtag = Etag.Parse(context.Request.QueryString["attachmentEtag"]);
				}
				catch
				{

				}
				Guid serverInstanceId;
				if (Guid.TryParse(context.Request.QueryString["dbid"], out serverInstanceId) == false)
					serverInstanceId = Database.TransactionalStorage.Id;

				if (document == null)
				{
					sourceReplicationInformation = new SourceReplicationInformation()
					{
						ServerInstanceId = serverInstanceId,
						LastAttachmentEtag = attachmentEtag ?? Etag.Empty,
						LastDocumentEtag = docEtag ?? Etag.Empty,
						Source = src
					};
				}
				else
				{
					sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
					sourceReplicationInformation.ServerInstanceId = serverInstanceId;
					sourceReplicationInformation.LastDocumentEtag = docEtag ?? sourceReplicationInformation.LastDocumentEtag;
					sourceReplicationInformation.LastAttachmentEtag = attachmentEtag ?? sourceReplicationInformation.LastAttachmentEtag;
				}

				var etag = document == null ? Etag.Empty : document.Etag;
				var metadata = document == null ? new RavenJObject() : document.Metadata;

				var newDoc = RavenJObject.FromObject(sourceReplicationInformation);
				log.Debug("Updating replication last etags from {0}: [doc: {1} attachment: {2}]", src,
								  sourceReplicationInformation.LastDocumentEtag,
								  sourceReplicationInformation.LastAttachmentEtag);
		
				Database.Put(Constants.RavenReplicationSourcesBasePath + "/" + src, etag, newDoc, metadata, null);
			}
		}

		public override string UrlPattern
		{
			get { return "^/replication/lastEtag$"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] { "GET", "PUT" }; }
		}
	}
}
