//-----------------------------------------------------------------------
// <copyright file="HttpListenerContextAdpater.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Net;
using System.Security.Principal;
using System.Text.RegularExpressions;
using NLog;
using Raven.Database.Config;
using Raven.Database.Util.Streams;

namespace Raven.Database.Server.Abstractions
{
	public class HttpListenerContextAdpater : IHttpContext, IDisposable
	{
		private readonly HttpListenerContext ctx;
		private readonly InMemoryRavenConfiguration configuration;
		private readonly IBufferPool bufferPool;
		private static readonly Regex maxAgeFinder = new Regex(@"max-age \s* = \s* (\d+)",
														   RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase |
														   RegexOptions.Compiled);

		public HttpListenerContextAdpater(HttpListenerContext ctx, InMemoryRavenConfiguration configuration, IBufferPool bufferPool)
		{
			this.ctx = ctx;
			this.configuration = configuration;
			this.bufferPool = bufferPool;
			ResponseInternal = new HttpListenerResponseAdapter(ctx.Response, bufferPool);
			RequestInternal = new HttpListenerRequestAdapter(ctx.Request);

			SetMaxAge();
		}

		private void SetMaxAge()
		{
			string cacheControl = ctx.Request.Headers["Cache-Control"];
			if (string.IsNullOrEmpty(cacheControl))
				return;

			var match = maxAgeFinder.Match(cacheControl);
			if (match.Success == false)
				return;

			int timeInSeconds;
			if (int.TryParse(match.Groups[1].Value, out timeInSeconds) == false)
				return;

			ctx.Response.AddHeader("Cache-Control", "max-age=" + timeInSeconds);
		}

		public bool RequiresAuthentication
		{
			get
			{
				return !(configuration.AllowLocalAccessWithoutAuthorization && ctx.Request.IsLocal);
			}
		}

		public InMemoryRavenConfiguration Configuration
		{
			get { return configuration; }
		}

		public IHttpRequest Request
		{
			get { return RequestInternal; }
		}

		protected HttpListenerResponseAdapter ResponseInternal { get; set; }
		protected HttpListenerRequestAdapter RequestInternal { get; set; }
		
		public IHttpResponse Response
		{
			get { return ResponseInternal; }
		}

		private IPrincipal internalUser;
		public IPrincipal User
		{
			get { return internalUser ?? ctx.User; }
			set { internalUser = value; }
		}

		public void FinalizeResonse()
		{
			try
			{
				Response.Close();
			}
			catch
			{
			}
		}

		public void SetResponseFilter(Func<Stream, Stream> responseFilter)
		{
			ResponseInternal.StreamsToDispose.Add(ResponseInternal.OutputStream);
			ResponseInternal.OutputStream = responseFilter(ResponseInternal.OutputStream);
		}


		public void SetRequestFilter(Func<Stream, Stream> requestFilter)
		{
			RequestInternal.InputStream = requestFilter(RequestInternal.InputStream);
		}

		private readonly List<Action<Logger>> loggedMessages = new List<Action<Logger>>();
		public void OutputSavedLogItems(Logger logger)
		{
			foreach (var loggedMessage in loggedMessages)
			{
				loggedMessage(logger);
			}
		}

		public void Log(Action<Logger> loggingAction)
		{
			loggedMessages.Add(loggingAction);
		}

		public string GetRequestUrlForTenantSelection()
		{
			return this.GetRequestUrl();
		}

		public void Dispose()
		{
			if (ResponseInternal != null)
				ResponseInternal.Dispose();

		}
	}
}