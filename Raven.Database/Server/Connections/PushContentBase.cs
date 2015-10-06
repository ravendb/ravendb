// -----------------------------------------------------------------------
//  <copyright file="PushContentBase.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.Connections
{
	public abstract class PushContentBase : HttpContent, IEventsTransport
	{
		private readonly DateTime _started = SystemTime.UtcNow;

		private readonly BlockingCollection<object> msgs = new BlockingCollection<object>(QueueCapacity);

		private const int QueueCapacity = 10000;

		private readonly ILog log = LogManager.GetCurrentClassLogger();

		private bool hitCapacity;

		public TimeSpan Age { get { return SystemTime.UtcNow - _started; } }

		public string Id { get; private set; }

		public bool Connected { get; set; }

		public event Action Disconnected = delegate { };

		protected PushContentBase()
		{
			Connected = true;
		}

		protected override async Task SerializeToStreamAsync(Stream stream, TransportContext context)
		{
			using (var writer = new StreamWriter(stream))
			{
				await writer.WriteAsync("data: { \"Type\": \"Heartbeat\" }\r\n\r\n").ConfigureAwait(false);
				await writer.FlushAsync().ConfigureAwait(false);

				while (Connected)
				{
					try
					{
						object message;
						while (msgs.TryTake(out message, millisecondsTimeout: 1000))
						{
							if (Connected == false)
								return;

							await SendMessage(message, writer).ConfigureAwait(false);
						}

						await writer.WriteAsync("data: { \"Type\": \"Heartbeat\" }\r\n\r\n").ConfigureAwait(false);
						await writer.FlushAsync().ConfigureAwait(false);
					}
					catch (Exception e)
					{
						Connected = false;
						if (log.IsDebugEnabled)
							log.DebugException("Error when using events transport", e);
						Disconnected();
						try
						{
							writer.WriteLine(e.ToString());
						}
						catch (Exception)
						{
							// try to send the information to the client, okay if they don't get it
							// because they might have already disconnected
						}
					}
				}
			}
		}

		private async Task SendMessage(object message, StreamWriter writer)
		{
			var o = JsonExtensions.ToJObject(message);
			await writer.WriteAsync("data: ").ConfigureAwait(false);
			await writer.WriteAsync(o.ToString(Formatting.None)).ConfigureAwait(false);
			await writer.WriteAsync("\r\n\r\n").ConfigureAwait(false);
			await writer.FlushAsync().ConfigureAwait(false);
		}

		protected override bool TryComputeLength(out long length)
		{
			length = 0;
			return false;
		}

		protected override void Dispose(bool disposing)
		{
			base.Dispose(disposing);
			Connected = false;
		}

		public void SendAsync(object msg)
		{
			if (msgs.TryAdd(msg) == false)
			{
				if (hitCapacity == false)
				{
					hitCapacity = true;
					log.Warn("Reached max capacity of HttpTrace queue, id = " + Id);
				}
			}
		}

		public string ResourceName { get; set; }

		public long CoolDownWithDataLossInMiliseconds { get; set; }
	}
}