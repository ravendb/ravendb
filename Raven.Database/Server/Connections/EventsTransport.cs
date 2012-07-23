// -----------------------------------------------------------------------
//  <copyright file="EventsTransport.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using NLog;
using Raven.Database.Server.Abstractions;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Connections
{
	public class EventsTransport
	{
		private readonly Logger log = LogManager.GetCurrentClassLogger();

		private readonly IHttpContext context;
		
		public string Id { get; private set; }
		public bool Connected { get; set; }

		public event Action Disconnected = delegate { }; 

		public EventsTransport(IHttpContext context)
		{
			this.context = context;
			Connected = true;
			Id = context.Request.QueryString["id"];
			if (string.IsNullOrEmpty(Id))
				throw new ArgumentException("Id is mandatory");
		}

		public Task ProcessAsync()
		{
			context.Response.ContentType = "text/event-stream";

			return SendAsync(new {Type ="InitializingConnetion"});
		}

		public Task SendAsync(object data)
		{
			var serializeObject = JsonConvert.SerializeObject(data, Formatting.None);
			log.Debug("Notifying {1}: {0}", serializeObject, Id);
			return context.Response.WriteAsync(serializeObject + "\r\n")
				.ContinueWith(DisconnectOnError);
		}

		public Task SendManyAsync(IEnumerable<object> data)
		{
			var sb = new StringBuilder();

			foreach (var o in data)
			{
				sb.Append(JsonConvert.SerializeObject(o))
					.Append("\r\n");
			}

			var s = sb.ToString();
			log.Debug("Notifying {1}: {0}", s, Id);
			return context.Response.WriteAsync(s)
				.ContinueWith(DisconnectOnError);
		}

		private void DisconnectOnError(Task prev)
		{
			prev.ContinueWith(task =>
			                  	{
			                  		if (task.IsFaulted == false) 
										return;
			                  		Connected = false;
			                  		Disconnected();
			                  		log.DebugException("Error when using events transport", task.Exception);

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
			Connected = false;
			Disconnected();
			context.FinalizeResonse();
		}
	}
}