using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using log4net;
using Newtonsoft.Json;
using Raven.Database;
using Raven.Database.Abstractions;
using Raven.Database.Exceptions;
using Raven.Database.Extensions;
using Raven.Database.Responders;
using Raven.Database.Storage;

namespace Raven.Server
{
	public class HttpServer : IDisposable
	{
		[ImportMany]
		public IEnumerable<RequestResponder> RequestResponders { get; set; }
		
		private readonly RavenConfiguration configuration;
		private HttpListener listener;

		private readonly ILog logger = LogManager.GetLogger(typeof (HttpServer));

		private int reqNum;

		// concurrent requests
		// we set 1/4 aside for handling background tasks
		private readonly SemaphoreSlim concurretRequestSemaphore = new SemaphoreSlim(TransactionalStorage.MaxSessions - (TransactionalStorage.MaxSessions/4));

		public HttpServer(RavenConfiguration configuration, DocumentDatabase database)
		{
			this.configuration = configuration;

			configuration.Container.SatisfyImportsOnce(this);

			foreach (var requestResponder in RequestResponders)
			{
				requestResponder.Database = database;
				requestResponder.Settings = configuration;
			}
		}

		#region IDisposable Members

		public void Dispose()
		{
			if (listener != null)
				listener.Stop();
		}

		#endregion

		public void Start()
		{
			listener = new HttpListener();
			listener.Prefixes.Add("http://+:" + configuration.Port + "/" + configuration.VirtualDirectory);
			switch (configuration.AnonymousUserAccessMode)
			{
				case AnonymousUserAccessMode.None:
					listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication;
					break;
				default:
					listener.AuthenticationSchemes = AuthenticationSchemes.IntegratedWindowsAuthentication |
						AuthenticationSchemes.Anonymous;
					break;
			}

			listener.Start();
			listener.BeginGetContext(GetContext, null);
		}

		private void GetContext(IAsyncResult ar)
		{
			IHttpContext ctx;
			try
			{
				ctx = new HttpListenerContextAdpater(listener.EndGetContext(ar));
				//setup waiting for the next request
				listener.BeginGetContext(GetContext, null);
			}
			catch (HttpListenerException)
			{
				// can't get current request / end new one, probably
				// listner shutdown
				return;
			}

			if (concurretRequestSemaphore.Wait(TimeSpan.FromSeconds(5)) == false)
			{
				HandleTooBusyError(ctx);
				return;
			}
			try
			{
				HandleActualRequest(ctx);
			}
			finally
			{
				concurretRequestSemaphore.Release();
			}
		}

		public void HandleActualRequest(IHttpContext ctx)
		{
			var curReq = Interlocked.Increment(ref reqNum);
			var sw = Stopwatch.StartNew();
			try
			{
				DispatchRequest(ctx);
			}
			catch (Exception e)
			{
				HandleException(ctx, e);
				logger.WarnFormat(e, "Error on request #{0}", curReq);
			}
			finally
			{
				try
				{
					ctx.Response.OutputStream.Flush();
					ctx.Response.Close();
				}
				catch
				{
				}
				logger.DebugFormat("Request #{0}: {1} {2} - {3}",
									   curReq, ctx.Request.HttpMethod, ctx.Request.Url.PathAndQuery, sw.Elapsed);
			}
		}

		private void HandleException(IHttpContext ctx, Exception e)
		{
			try
			{
				if (e is BadRequestException)
					HandleBadRequest(ctx, (BadRequestException)e);
				else if (e is ConcurrencyException)
					HandleConcurrencyException(ctx, (ConcurrencyException)e);
				else if (e is IndexDisabledException)
					HandleIndexDisabledException(ctx, (IndexDisabledException)e);
				else
					HandleGenericException(ctx, e);
			}
			catch (Exception)
			{
				logger.Error("Failed to properly handle error, further error handling is ignored", e);
			}
		}

		private static void HandleTooBusyError(IHttpContext ctx)
		{
			ctx.Response.StatusCode = 503;
			ctx.Response.StatusDescription = "Service Unavailable";
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				Error = "The server is too busy, could not acquire transactional access"
			});
		}

		private static void HandleIndexDisabledException(IHttpContext ctx, IndexDisabledException e)
		{
			ctx.Response.StatusCode = 503;
			ctx.Response.StatusDescription = "Service Unavailable";
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				Error = e.Information.GetErrorMessage(),
				Index = e.Information.Name,
			});
		}

		private static void HandleGenericException(IHttpContext ctx, Exception e)
		{
			ctx.Response.StatusCode = 500;
			ctx.Response.StatusDescription = "Internal Server Error";
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				Error = e.ToString()
			});
		}

		private static void HandleBadRequest(IHttpContext ctx, BadRequestException e)
		{
			ctx.SetStatusToBadRequest();
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				e.Message,
				Error = e.Message
			});
		}

		private static void HandleConcurrencyException(IHttpContext ctx, ConcurrencyException e)
		{
			ctx.Response.StatusCode = 409;
			ctx.Response.StatusDescription = "Conflict";
			SerializeError(ctx, new
			{
				Url = ctx.Request.RawUrl,
				e.ActualETag,
				e.ExpectedETag,
				Error = e.Message
			});
		}

		private static void SerializeError(IHttpContext ctx, object error)
		{
			using (var sw = new StreamWriter(ctx.Response.OutputStream))
			{
				new JsonSerializer().Serialize(new JsonTextWriter(sw)
				{
					Formatting = Formatting.Indented,
				}, error);
			}
		}

		private void DispatchRequest(IHttpContext ctx)
		{
			if (AssertSecurityRights(ctx) == false)
				return;

			foreach (var requestResponder in RequestResponders)
			{
				if (requestResponder.WillRespond(ctx))
				{
					requestResponder.Respond(ctx);
					return;
				}
			}
			ctx.SetStatusToBadRequest();
			ctx.Write(
				@"
<html>
    <body>
        <h1>Could not figure out what to do</h1>
        <p>Your request didn't match anything that Raven knows to do, sorry...</p>
    </body>
</html>
");
		}

		private bool AssertSecurityRights(IHttpContext ctx)
		{
			if (configuration.AnonymousUserAccessMode == AnonymousUserAccessMode.Get &&
				(ctx.User == null || ctx.User.Identity == null || ctx.User.Identity.IsAuthenticated == false) &&
					ctx.Request.HttpMethod != "GET"
				)
			{
				ctx.SetStatusToUnauthorized();
				return false;
			}
			return true;
		}
	}
}