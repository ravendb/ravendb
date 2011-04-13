namespace Raven.Studio.Features.Tasks
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.IO;
	using System.Net;
	using System.Threading.Tasks;
	using System.Windows.Controls;
	using Caliburn.Micro;
	using Client.Client;
	using Client.Document;
	using Client.Silverlight.Client;
	using Database;
	using Ionic.Zlib;
	using Framework.Extensions;
	using Messages;
	using Newtonsoft.Json;
	using Newtonsoft.Json.Linq;
	using Plugins;

	[Plugins.Tasks.ExportTask("Export Database")]
	public class ExportTask : ConsoleOutputTask
	{
		bool exportIndexesOnly;

		[ImportingConstructor]
		public ExportTask(IServer server, IEventAggregator events)
			: base(server, events)
		{
		}

		public bool ExportIndexesOnly
		{
			get { return exportIndexesOnly; }
			set
			{
				exportIndexesOnly = value;
				NotifyOfPropertyChange(() => ExportIndexesOnly);
			}
		}

		public void ExportData()
		{
			Status = string.Empty;

			var saveFile = new SaveFileDialog();
			var dialogResult = saveFile.ShowDialog();

			if (!dialogResult.HasValue || !dialogResult.Value) return;

			WorkStarted("exporting database");
	
			var tasks = (IEnumerable<Task>)ExportData(saveFile, ExportIndexesOnly).GetEnumerator();
			tasks.ExecuteInSequence(OnTaskFinished, OnException);
		}

		void OnTaskFinished(bool success)
		{
			WorkCompleted("exporting database");

			Status = success ? "Export Complete" : "Export Failed!";

			if(success)
				Events.Publish(new NotificationRaised("Export Completed", NotificationLevel.Info));
		}

		void OnException(Exception e)
		{
			Output("The export failed with the following exception: {0}", e.Message);
			NotifyError("Database Export Failed");
		}

		IEnumerable<Task> ExportData(SaveFileDialog saveFile, bool indexesOnly)
		{
			Output("Exporting to {0}", saveFile.SafeFileName);
			Output("- Indexes only, documents will be excluded");

			var stream = saveFile.OpenFile();
			var jsonRequestFactory = new HttpJsonRequestFactory();
			var baseUrl = server.CurrentDatabaseAddress;
			var credentials = new NetworkCredential();
			var convention = new DocumentConvention();

			var streamWriter = new StreamWriter(new GZipStream(stream, CompressionMode.Compress));
			var jsonWriter = new JsonTextWriter(streamWriter)
								{
									Formatting = Formatting.Indented
								};

			Output("Begin reading indexes");

			jsonWriter.WriteStartObject();
			jsonWriter.WritePropertyName("Indexes");
			jsonWriter.WriteStartArray();

			int totalCount = 0;
			const int batchSize = 128;
			var completed = false;

			while (!completed)
			{
				var url = (baseUrl + "/indexes/?start=" + totalCount + "&pageSize=" + batchSize).NoCache();
				var request = jsonRequestFactory.CreateHttpJsonRequest(this, url, "GET", credentials, convention);
				var response = request.ReadResponseStringAsync();
				yield return response;

				var documents = response.Result;
				var array = JArray.Parse(documents);
				if (array.Count == 0)
				{
					Output("Done with reading indexes, total: {0}", totalCount);
					completed = true;
				}
				else
				{
					totalCount += array.Count;
					Output("Reading batch of {0,3} indexes, read so far: {1,10:#,#}", array.Count, totalCount);
					foreach (JToken item in array)
					{
						item.WriteTo(jsonWriter);
					}
				}
			}

			jsonWriter.WriteEndArray();
			jsonWriter.WritePropertyName("Docs");
			jsonWriter.WriteStartArray();

			if (indexesOnly)
			{
				Output("Skipping documents");
			}
			else
			{
				Output("Begin reading documents");

				var lastEtag = Guid.Empty;
				totalCount = 0;
				completed = false;
				while (!completed)
				{
					var url = (baseUrl + "/docs/?pageSize=" + batchSize + "&etag=" + lastEtag).NoCache();
					var request = jsonRequestFactory.CreateHttpJsonRequest(this, url, "GET", credentials, convention);
					var response = request.ReadResponseStringAsync();
					yield return response;

					var array = JArray.Parse(response.Result);
					if (array.Count == 0)
					{
						Output("Done with reading documents, total: {0}", totalCount);
						completed = true;
					}
					else
					{
						totalCount += array.Count;
						Output("Reading batch of {0,3} documents, read so far: {1,10:#,#}", array.Count,
									totalCount);
						foreach (JToken item in array)
						{
							item.WriteTo(jsonWriter);
						}
						lastEtag = new Guid(array.Last.Value<JObject>("@metadata").Value<string>("@etag"));
					}
				}
			}

			Execute.OnUIThread(() =>
								{
									jsonWriter.WriteEndArray();
									jsonWriter.WriteEndObject();
									streamWriter.Flush();
									streamWriter.Dispose();
									stream.Dispose();
								});
			Output("Export complete");
		}
	}
}