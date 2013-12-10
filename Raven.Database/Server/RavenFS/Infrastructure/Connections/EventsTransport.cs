using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using NLog;
using Raven.Client.RavenFS;
using Raven.Database.Server.RavenFS.Notifications;
using Raven.Database.Server.RavenFS.Util;
using Raven.Imports.Newtonsoft.Json;

namespace Raven.Database.Server.RavenFS.Infrastructure.Connections
{
	/// <summary>
	///     Responsible for streaming events to an individual client
	/// </summary>
	/// <remarks>
	///     We use a queue to serialize the writing of individual messages to the stream because we were encountering concurrency exceptions when
	///     a heartbeat and a message where being written to the stream at the same time
	/// </remarks>
	public class EventsTransport
	{
		private static readonly JsonSerializerSettings Settings;
		private readonly Timer heartbeat;

		private readonly Logger log = LogManager.GetCurrentClassLogger();

		private readonly AwaitableQueue<Tuple<string, TaskCompletionSource<bool>>> messageQueue =
			new AwaitableQueue<Tuple<string, TaskCompletionSource<bool>>>();

		private volatile bool connected;

		static EventsTransport()
		{
			Settings = new JsonSerializerSettings
			{
				Binder = new TypeHidingBinder(),
				TypeNameHandling = TypeNameHandling.All,
			};
		}

		public EventsTransport(string id)
		{
			connected = true;
			Id = id;
			if (string.IsNullOrEmpty(Id))
				throw new ArgumentException("Id is mandatory");

			heartbeat = new Timer(Heartbeat);
		}

		public string Id { get; private set; }

		public bool Connected
		{
			get { return connected; }
		}

		public event Action Disconnected = delegate { };

		public HttpResponseMessage GetResponse()
		{
			var response = new HttpResponseMessage { Content = new PushStreamContent((Action<Stream, HttpContent, TransportContext>) HandleStreamAvailable, "text/event-stream") };

			SendAsync(new Heartbeat());

			return response;
		}

		private void HandleStreamAvailable(Stream stream, HttpContent content, TransportContext context)
		{
			heartbeat.Change(TimeSpan.FromSeconds(5), TimeSpan.FromSeconds(5));
			ProcessMessageQueue(stream).Wait();
		}

		private async Task ProcessMessageQueue(Stream stream)
		{
			try
			{
				// if Disconnect() is called DequeueOrWaitAsync will throw OperationCancelledException, 
				var messageWithTask = await messageQueue.DequeueOrWaitAsync();
				using (var streamWriter = new StreamWriter(stream))
				{
					while (Connected)
					{
						var taskCompletionSource = messageWithTask.Item2;
						var messageContent = messageWithTask.Item1;

						try
						{
							await streamWriter.WriteAsync(messageContent);
							await streamWriter.FlushAsync();
							taskCompletionSource.SetResult(true);
						}
						catch (Exception ex)
						{
							log.DebugException("Error when using events transport", ex);

							taskCompletionSource.SetException(ex);
							Disconnect();
							SignalErrorToQueuedTasks(ex);
							break;
						}

						messageWithTask = await messageQueue.DequeueOrWaitAsync();
					}
				}
			}
			catch (OperationCanceledException)
			{
				SignalCancelledToQueuedTasks();
			}
			finally
			{
				heartbeat.Dispose();
				Disconnected();
				CloseTransport(stream);
			}
		}

		private void SignalErrorToQueuedTasks(Exception ex)
		{
			Tuple<string, TaskCompletionSource<bool>> messageWithTask;

			while (messageQueue.TryDequeue(out messageWithTask))
			{
				messageWithTask.Item2.SetException(ex);
			}
		}

		private void SignalCancelledToQueuedTasks()
		{
			Tuple<string, TaskCompletionSource<bool>> messageWithTask;

			while (messageQueue.TryDequeue(out messageWithTask))
			{
				messageWithTask.Item2.SetCanceled();
			}
		}

		private void Heartbeat(object _)
		{
			SendAsync(new Heartbeat());
		}

		public Task SendAsync(Notification data)
		{
			var content = "data: " + JsonConvert.SerializeObject(data, Formatting.None, Settings) + "\r\n\r\n";

			return Enqueue(content);
		}

		private Task Enqueue(string message)
		{
			var tcs = new TaskCompletionSource<bool>();

			if (!messageQueue.TryEnqueue(Tuple.Create(message, tcs)))
				tcs.SetCanceled();

			return tcs.Task;
		}

		private void CloseTransport(Stream stream)
		{
			try
			{
				using (stream)
				{
				}
			}
			catch (Exception closeEx)
			{
				log.DebugException("Could not close transport", closeEx);
			}
		}

		public Task SendManyAsync(IEnumerable<Notification> data)
		{
			var sb = new StringBuilder();

			foreach (var o in data)
			{
				sb.Append("data: ")
				  .Append(JsonConvert.SerializeObject(o, Formatting.None, Settings))
				  .Append("\r\n\r\n");
			}
			var content = sb.ToString();

			return Enqueue(content);
		}

		public void Disconnect()
		{
			connected = false;
			messageQueue.SignalCompletion();
		}
	}
}
