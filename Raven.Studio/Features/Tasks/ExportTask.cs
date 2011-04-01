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
	using Newtonsoft.Json;
	using Newtonsoft.Json.Linq;

	[ExportTask("Export Database")]
	public class ExportTask : Screen, ITask
	{
		readonly IServer server;

		bool exportIndexesOnly;

		[ImportingConstructor]
		public ExportTask(IServer server)
		{
			this.server = server;

			Console = new BindableCollection<string>();
			server.CurrentDatabaseChanged += delegate { ClearConsole(); };
		}

		public IObservableCollection<string> Console { get; private set; }

		public void ClearConsole()
		{
			Console.Clear();
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
			var saveFile = new SaveFileDialog();
			var dialogResult = saveFile.ShowDialog();

			if (!dialogResult.HasValue || !dialogResult.Value) return;

			var tasks = (IEnumerable<Task>) ExportData(saveFile, ExportIndexesOnly).GetEnumerator();
			tasks.ExecuteInSequence(null);
		}

		IEnumerable<Task> ExportData(SaveFileDialog saveFile, bool indexesOnly)
		{
			Console.Add("Exporting to {0}",saveFile.SafeFileName);

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

			Console.Add("Begin reading indexes");

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
					Console.Add("Done with reading indexes, total: {0}", totalCount);
					completed = true;
				}
				else
				{
					totalCount += array.Count;
					Console.Add("Reading batch of {0,3} indexes, read so far: {1,10:#,#}", array.Count, totalCount);
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
				Console.Add("Documents will not be exported.");
			} 
			else 
			{
				Console.Add("Begin reading documents.");

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
						Console.Add("Done with reading documents, total: {0}", totalCount);
						completed = true;
					}
					else
					{
						totalCount += array.Count;
						Console.Add("Reading batch of {0,3} documents, read so far: {1,10:#,#}", array.Count,
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
		}
	}
}