namespace Raven.Studio.Features.Tasks
{
	using System;
	using System.Collections.Generic;
	using System.ComponentModel.Composition;
	using System.Diagnostics;
	using System.IO;
	using System.Linq;
	using System.Text;
	using System.Threading.Tasks;
	using System.Windows.Controls;
	using Caliburn.Micro;
	using Database;
	using Ionic.Zlib;
	using Framework.Extensions;
	using Newtonsoft.Json;
	using Newtonsoft.Json.Linq;
	using Raven.Database.Data;
	using Raven.Database.Indexing;

	[ExportTask("Import Database")]
	public class ImportTask : ConsoleOutputTask, ITask
	{
		[ImportingConstructor]
		public ImportTask(IServer server, IEventAggregator events)
			: base(server, events)
		{
		}

		public void ImportData()
		{
			var openFile = new OpenFileDialog();
			var dialogResult = openFile.ShowDialog();

			if (!dialogResult.HasValue || !dialogResult.Value) return;

			try
			{
				var tasks = (IEnumerable<Task>)ImportData(openFile).GetEnumerator();
				tasks.ExecuteInSequence(null);
			}
			catch (InvalidDataException e)
			{
				Output("The import file was not formatted correctly:\n\t{0}", e.Message);
				Output("Import terminated.");
			}
		}

		IEnumerable<Task> ImportData(OpenFileDialog openFile)
		{
			Output("Importing from {0}", openFile.File.Name);

			var sw = Stopwatch.StartNew();

			var stream = openFile.File.OpenRead();
			// Try to read the stream compressed, otherwise continue uncompressed.
			JsonTextReader jsonReader;

			try
			{
				var streamReader = new StreamReader(new GZipStream(stream, CompressionMode.Decompress));

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false) yield break;
			}
			catch (Exception)
			{
				Output("Import file did not use GZip compression, attempting to read as uncompressed.");

				stream.Seek(0, SeekOrigin.Begin);

				var streamReader = new StreamReader(stream);

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false) yield break;
			}

			if (jsonReader.TokenType != JsonToken.StartObject)
				throw new InvalidDataException("StartObject was expected");

			// should read indexes now
			if (jsonReader.Read() == false)
				yield break;

			Output("Begin reading indexes");

			if (jsonReader.TokenType != JsonToken.PropertyName)
				throw new InvalidDataException("PropertyName was expected");

			if (Equals("Indexes", jsonReader.Value) == false)
				throw new InvalidDataException("Indexes property was expected");

			if (jsonReader.Read() == false)
				yield break;

			if (jsonReader.TokenType != JsonToken.StartArray)
				throw new InvalidDataException("StartArray was expected");

			// import Indexes
			var totalIndexes = 0;
			using (var session = server.OpenSession())
				while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
				{
					var json = JToken.ReadFrom(jsonReader);
					var indexName = json.Value<string>("name");
					if (indexName.StartsWith("Raven/"))
						continue;

					var index = JsonConvert.DeserializeObject<IndexDefinition>(json.Value<JObject>("definition").ToString());

					totalIndexes++;

					Output("Importing index: {0}", indexName);

					yield return session.Advanced.AsyncDatabaseCommands
						.PutIndexAsync(indexName, index, overwrite: true);
				}

			Output("Imported {0:#,#} indexes", totalIndexes);

			Output("Begin reading documents");

			// should read documents now
			if (jsonReader.Read() == false)
				yield break;

			if (jsonReader.TokenType != JsonToken.PropertyName)
				throw new InvalidDataException("PropertyName was expected");

			if (Equals("Docs", jsonReader.Value) == false)
				throw new InvalidDataException("Docs property was expected");

			if (jsonReader.Read() == false)
				yield break;

			if (jsonReader.TokenType != JsonToken.StartArray)
				throw new InvalidDataException("StartArray was expected");

			var batch = new List<JObject>();
			int totalCount = 0;
			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				totalCount += 1;
				var document = JToken.ReadFrom(jsonReader);
				batch.Add((JObject)document);
				if (batch.Count >= 128)
					yield return FlushBatch(batch);
			}

			yield return FlushBatch(batch);

			Output("Imported {0:#,#} documents in {1:#,#} ms", totalCount, sw.ElapsedMilliseconds);
		}

		Task FlushBatch(List<JObject> batch)
		{
			var sw = Stopwatch.StartNew();
			long size = 0;

			var commands = (from doc in batch
							let metadata = doc.Value<JObject>("@metadata")
							let removal = doc.Remove("@metadata")
							select new PutCommandData
									{
										Metadata = metadata,
										Document = doc,
										Key = metadata.Value<string>("@id"),
									}).ToArray();


			//TODO: all of this is just to get the size; I suspect there is a Better Way
			using (var stream = new MemoryStream())
			{
				using (var streamWriter = new StreamWriter(stream, Encoding.UTF8))
				using (var jsonTextWriter = new JsonTextWriter(streamWriter))
				{
					commands.Apply(_ => _.ToJson().WriteTo(jsonTextWriter));
					jsonTextWriter.Flush();
					streamWriter.Flush();
					stream.Flush();
					size = stream.Length;
				}
			}

			Output("Wrote {0} documents [{1:#,#} kb] in {2:#,#} ms",
						batch.Count, Math.Round((double)size / 1024, 2), sw.ElapsedMilliseconds);
			batch.Clear();

			return server.OpenSession().Advanced.AsyncDatabaseCommands
				.BatchAsync(commands);
		}
	}
}