//-----------------------------------------------------------------------
// <copyright file="HttpListenerContextAdpater.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Security.Principal;
using System.Text.RegularExpressions;
using NLog;

namespace Raven.Http.Abstractions
{
	public class HttpListenerContextAdpater : IHttpContext
	{
		private readonly HttpListenerContext ctx;
		private readonly IRavenHttpConfiguration configuration;
		private static readonly Regex maxAgeFinder = new Regex(@"max-age \s* = \s* (\d+)",
														   RegexOptions.IgnorePatternWhitespace | RegexOptions.IgnoreCase |
														   RegexOptions.Compiled);
	  
		public HttpListenerContextAdpater(HttpListenerContext ctx, IRavenHttpConfiguration configuration)
		{
			this.ctx = ctx;
			this.configuration = configuration;
			Request = new HttpListenerRequestAdapter(ctx.Request);
			ResponseInternal = new HttpListenerResponseAdapter(ctx.Response);

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

		public IRavenHttpConfiguration Configuration
		{
			get { return configuration; }
		}

		public IHttpRequest Request
		{
			get;
			set;
		}

		protected HttpListenerResponseAdapter ResponseInternal { get; set; }
		
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
				ResponseInternal.OutputStream.Flush();
				ResponseInternal.OutputStream.Dispose(); // this is required when using compressing stream
				ctx.Response.Close();
			}
			catch
			{
			}
		}

		public void SetResponseFilter(Func<Stream, Stream> responseFilter)
		{
			ResponseInternal.OutputStream = responseFilter(ResponseInternal.OutputStream);
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
	}
}
