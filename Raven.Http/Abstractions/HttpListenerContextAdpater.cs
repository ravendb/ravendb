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
using log4net;

namespace Raven.Http.Abstractions
{
	public class HttpListenerContextAdpater : IHttpContext
	{
		private readonly HttpListenerContext ctx;
        private readonly IRaveHttpnConfiguration configuration;

        public HttpListenerContextAdpater(HttpListenerContext ctx, IRaveHttpnConfiguration configuration)
		{
			this.ctx = ctx;
			this.configuration = configuration;
			Request = new HttpListenerRequestAdapter(ctx.Request);
			ResponseInternal = new HttpListenerResponseAdapter(ctx.Response);
		}

        public IRaveHttpnConfiguration Configuration
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

		public IPrincipal User
		{
			get { return ctx.User; }
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

		private readonly List<Action<ILog>> loggedMessages = new List<Action<ILog>>();
		public void OutputSavedLogItems(ILog logger)
		{
			foreach (var loggedMessage in loggedMessages)
			{
				loggedMessage(logger);
			}
		}

		public void Log(Action<ILog> loggingAction)
		{
			loggedMessages.Add(loggingAction);
		}
	}
}
