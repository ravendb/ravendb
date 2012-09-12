//-----------------------------------------------------------------------
// <copyright file="ReplicationLastEtagResponser.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.ComponentModel.Composition;
using NLog;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Bundles.Replication.Data;
using Raven.Database.Extensions;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;
using Raven.Json.Linq;

namespace Raven.Bundles.Replication.Responders
{
	[ExportMetadata("Bundle", "Replication")]
	[InheritedExport(typeof(AbstractRequestResponder))]
	public class ReplicationLastEtagResponser : AbstractRequestResponder
	{
		private Logger log = LogManager.GetCurrentClassLogger();

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

				if (document == null)
				{
					sourceReplicationInformation = new SourceReplicationInformation()
					{
						ServerInstanceId = Database.TransactionalStorage.Id,
					};
				}
				else
				{
					sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
					sourceReplicationInformation.ServerInstanceId = Database.TransactionalStorage.Id;
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

				Guid? docEtag = null, attachmentEtag = null;
				Guid val;
				if(Guid.TryParse(context.Request.QueryString["docEtag"], out val))
				{
					docEtag = val;
				}
				if(Guid.TryParse(context.Request.QueryString["attachmentEtag"], out val))
				{
					attachmentEtag = val;
				}

				if (document == null)
				{
					sourceReplicationInformation = new SourceReplicationInformation()
					{
						ServerInstanceId = Database.TransactionalStorage.Id,
						LastAttachmentEtag = attachmentEtag ?? Guid.Empty,
						LastDocumentEtag = docEtag??Guid.Empty
					};
				}
				else
				{
					sourceReplicationInformation = document.DataAsJson.JsonDeserialization<SourceReplicationInformation>();
					sourceReplicationInformation.ServerInstanceId = Database.TransactionalStorage.Id;
					sourceReplicationInformation.LastDocumentEtag = docEtag ?? sourceReplicationInformation.LastDocumentEtag;
					sourceReplicationInformation.LastAttachmentEtag = attachmentEtag ?? sourceReplicationInformation.LastAttachmentEtag;
				}

				var etag = document == null ? Guid.Empty : document.Etag;
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
