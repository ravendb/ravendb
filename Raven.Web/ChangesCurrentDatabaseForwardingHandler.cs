// -----------------------------------------------------------------------
//  <copyright file="ChangesCurrentDatabaseForwardingHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using System.Web;
using Raven.Abstractions.Util;
using Raven.Database.Server;
using Raven.Database.Server.Abstractions;

namespace Raven.Web
{
	public class ChangesCurrentDatabaseForwardingHandler : IHttpAsyncHandler
	{
		private readonly HttpServer server;

		public ChangesCurrentDatabaseForwardingHandler(HttpServer server)
		{
			this.server = server;
		}

		public Task ProcessRequestAsync(HttpContext context)
		{
			var tcs = new TaskCompletionSource<object>();
			server.HandleChangesRequest(new HttpContextAdapter(HttpContext.Current, server.Configuration), ()=> tcs.TrySetResult(null))
				.ContinueWith(task =>
				              	{
				              		if(task.IsFaulted)
				              			tcs.TrySetException(task.Exception);
									else if (task.IsCanceled)
										tcs.TrySetCanceled();
				              	});
			return tcs.Task;
		}

		public void ProcessRequest(HttpContext context)
		{
			ProcessRequestAsync(context).Wait();
		}

		public bool IsReusable
		{
			get { return false; }
		}

		public IAsyncResult BeginProcessRequest(HttpContext context, AsyncCallback cb, object extraData)
		{
			context.Response.BufferOutput = false;
			return ProcessRequestAsync(context)
				.ContinueWith(task => cb(task));
		}

		public void EndProcessRequest(IAsyncResult result)
		{
			var task = result as Task;
			if (task != null)
				task.Wait(); // ensure we get proper errors
		}
	}
}