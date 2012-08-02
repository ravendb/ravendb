// -----------------------------------------------------------------------
//  <copyright file="EventsTransport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Raven.Database.Server.Abstractions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Connections
{
	public class EventsTransport
	{
		private readonly Timer heartbeat;

		private readonly Logger log = LogManager.GetCurrentClassLogger();

		private readonly IHttpContext context;
		
		public string Id { get; private set; }
		public bool Connected { get; set; }

		public event Action Disconnected = delegate { };

		private Task InitTask;

		public EventsTransport(IHttpContext context)
		{
			this.context = context;
			Connected = true;
			Id = context.Request.QueryString["id"];
			if (string.IsNullOrEmpty(Id))
				throw new ArgumentException("Id is mandatory");

			heartbeat = new Timer(Heartbeat);
	
		}

		public Task ProcessAsync()
		{
			context.Response.ContentType = "text/event-stream";
			InitTask = SendAsync(new { Type = "Initialized" });
			Thread.MemoryBarrier();
			heartbeat.Change(TimeSpan.Zero, TimeSpan.FromSeconds(5));

			return InitTask;

		}

		private void Heartbeat(object _)
		{
			SendAsync(new { Type = "Heartbeat" });
		}

		public Task SendAsync(object data)
		{
			if (InitTask != null && // may be the very first time? 
				InitTask.IsCompleted == false) // still pending on this...
				return InitTask.ContinueWith(_ => SendAsync(data)).Unwrap();


			return context.Response.WriteAsync("data: " + JsonConvert.SerializeObject(data,Formatting.None) + "\r\n\r\n")
				.ContinueWith(DisconnectOnError);
		}

		public Task SendManyAsync(IEnumerable<object> data)
		{
			if (InitTask.IsCompleted == false)
				return InitTask.ContinueWith(_ => SendManyAsync(data)).Unwrap();

			var sb = new StringBuilder();

			foreach (var o in data)
			{
				sb.Append("data: ")
					.Append(JsonConvert.SerializeObject(o))
					.Append("\r\n\r\n");
			}

			return context.Response.WriteAsync(sb.ToString())
				.ContinueWith(DisconnectOnError);
		}

		private void DisconnectOnError(Task prev)
		{
			prev.ContinueWith(task =>
			                  	{
			                  		if (task.IsFaulted == false) 
										return;
									log.DebugException("Error when using events transport", task.Exception);
									
			                  		Connected = false;
			                  		Disconnected();
			                  		try
			                  		{
										context.FinalizeResonse();
			                  		}
			                  		catch (Exception e)
			                  		{
			                  			log.DebugException("Could not close transport", e);
			                  		}
			                  	});
		}

		public void Disconnect()
		{
			if (heartbeat != null)
				heartbeat.Dispose();
			
			Connected = false;
			Disconnected();
			context.FinalizeResonse();
		}
	}
}
