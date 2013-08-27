//-----------------------------------------------------------------------
// <copyright file="Static.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using Raven.Abstractions.Extensions;
using Raven.Database.Data;
using Raven.Database.Extensions;
using Raven.Database.Server.Abstractions;

namespace Raven.Database.Server.Responders
{
	public class Static : AbstractRequestResponder
	{
		public override string UrlPattern
		{
			get { return "^/static/(.+)"; }
		}

		public override string[] SupportedVerbs
		{
			get { return new[] {"GET", "PUT", "POST", "DELETE","HEAD"}; }
		}

		public override void Respond(IHttpContext context)
		{
			var match = urlMatcher.Match(context.GetRequestUrl());
			var filename = match.Groups[1].Value;
			var etag = context.GetEtag();
			switch (context.Request.HttpMethod)
			{
				case "GET":
					Database.TransactionalStorage.BatchRead(_=> // have to keep the session open for reading of the attachment stream
					{
						var attachmentAndHeaders = Database.GetStatic(filename);
						if (attachmentAndHeaders == null)
						{
							context.SetStatusToNotFound();
							return;
						}
						if (context.MatchEtag(attachmentAndHeaders.Etag))
						{
							context.SetStatusToNotModified();
							return;
						}
						context.WriteHeaders(attachmentAndHeaders.Metadata, attachmentAndHeaders.Etag);
						using (var stream = attachmentAndHeaders.Data())
						{
							stream.CopyTo(context.Response.OutputStream);
						}
					});
					break;
				case "HEAD":
					Database.TransactionalStorage.BatchRead(_ => // have to keep the session open for reading of the attachment stream
					{
						var attachmentAndHeaders = Database.GetStatic(filename);
						if (attachmentAndHeaders == null)
						{
							context.SetStatusToNotFound();
							return;
						}
						if (context.MatchEtag(attachmentAndHeaders.Etag))
						{
							context.SetStatusToNotModified();
							return;
						}
						context.WriteHeaders(attachmentAndHeaders.Metadata, attachmentAndHeaders.Etag);
						context.Response.ContentLength64 = attachmentAndHeaders.Size;
					});
					break;
				case "PUT":
					var newEtag = Database.PutStatic(filename, context.GetEtag(), context.Request.InputStream,
					                                 context.Request.Headers.FilterHeadersAttachment());

					context.WriteETag(newEtag);
					context.SetStatusToCreated("/static/" + Uri.EscapeUriString(filename));
					break;

				case "POST":
					var newEtagPost = Database.PutStatic(filename, context.GetEtag(), null,
													 context.Request.Headers.FilterHeadersAttachment());

					context.WriteETag(newEtagPost);
					break;
				case "DELETE":
					Database.DeleteStatic(filename, etag);
					context.SetStatusToDeleted();
					break;
			}
		}
	}
}
