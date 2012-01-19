using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;
using Ionic.Zlib;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Silverlight.Connection;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class ExportDatabaseCommand : Command
	{
		const int BatchSize = 512;

		private readonly Action<string> output;
		private Stream stream;
		private GZipStream gZipStream;
		private StreamWriter streamWriter;
		private JsonTextWriter jsonWriter;

		public ExportDatabaseCommand(Action<string> output)
		{
			this.output = output;
		}

		public override void Execute(object parameter)
		{
			var saveFile = new SaveFileDialog
			               {
			               	/*TODO, In Silverlight 5: DefaultFileName = string.Format("Dump of {0}, {1}", ApplicationModel.Database.Value.Name, DateTimeOffset.Now.ToString()), */
			               	DefaultExt = ".raven.dump",
			               	Filter = "Raven Dumps|*.raven.dump",
			               };

			if (saveFile.ShowDialog() != true)
				return;

			stream = saveFile.OpenFile();
			gZipStream = new GZipStream(stream, CompressionMode.Compress);
			streamWriter = new StreamWriter(gZipStream);
			jsonWriter = new JsonTextWriter(streamWriter)
			             {
			             	Formatting = Formatting.Indented
			             };

			output(String.Format("Exporting to {0}", saveFile.SafeFileName));

			output("Begin reading indexes");

			jsonWriter.WriteStartObject();
			jsonWriter.WritePropertyName("Indexes");
			jsonWriter.WriteStartArray();

			ReadIndexes(0).Catch(exception => Infrastructure.Execute.OnTheUI(() => Finish(exception)));
		}

		private Task ReadIndexes(int totalCount)
		{
			var url = ("/indexes/?start=" + totalCount + "&pageSize=" + BatchSize).NoCache();
			var request = DatabaseCommands.CreateRequest(url, "GET");
			return request.ReadResponseJsonAsync()
				.ContinueOnSuccess(documents =>
				                   	{
				                   		var array = ((RavenJArray)documents);
				                   		if (array.Length == 0)
				                   		{
				                   			output(String.Format("Done with reading indexes, total: {0}", totalCount));

				                   			jsonWriter.WriteEndArray();
				                   			jsonWriter.WritePropertyName("Docs");
				                   			jsonWriter.WriteStartArray();

				                   			return ReadDocuments(Guid.Empty, 0);
				                   		}
				                   		else
				                   		{
				                   			totalCount += array.Length;
				                   			output(String.Format("Reading batch of {0,3} indexes, read so far: {1,10:#,#;;0}", array.Length,
				                   			                     totalCount));
				                   			foreach (RavenJToken item in array)
				                   			{
				                   				item.WriteTo(jsonWriter);
				                   			}

				                   			return ReadIndexes(totalCount);
				                   		}
				                   	});
		}

		private Task ReadDocuments(Guid lastEtag, int totalCount)
		{
			var url = ("/docs/?pageSize=" + BatchSize + "&etag=" + lastEtag).NoCache();
			var request = DatabaseCommands.CreateRequest(url, "GET");
			return request.ReadResponseJsonAsync()
				.ContinueOnSuccess(docs =>
				                   	{
				                   		var array = ((RavenJArray)docs);
				                   		if (array.Length == 0)
				                   		{
				                   			output(String.Format("Done with reading documents, total: {0}", totalCount));
				                   			jsonWriter.WriteEndArray();
				                   			jsonWriter.WriteEndObject();

				                   			return Infrastructure.Execute.OnTheUI(() => Finish(null));
				                   		}
				                   		else
				                   		{
				                   			totalCount += array.Length;
				                   			output(String.Format("Reading batch of {0,3} documents, read so far: {1,10:#,#;;0}", array.Length,
				                   			                     totalCount));
				                   			foreach (RavenJToken item in array)
				                   			{
				                   				item.WriteTo(jsonWriter);
				                   			}
				                   			lastEtag = new Guid(array.Last().Value<JObject>("@metadata").Value<string>("@etag"));

				                   			return ReadDocuments(lastEtag, totalCount);
				                   		}
				                   	});


		}

		private void Finish(Exception exception)
		{
			streamWriter.Flush();
			streamWriter.Dispose();
			stream.Dispose();

			output("Export complete");
			if (exception != null)
				output(exception.ToString());
		}
	}
}