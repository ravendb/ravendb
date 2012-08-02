using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;
using Ionic.Zlib;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using TaskStatus = Raven.Studio.Models.TaskStatus;

namespace Raven.Studio.Commands
{
	public class ImportDatabaseCommand : Command
	{
		const int BatchSize = 512;

		private readonly Action<string> output;
		private int totalCount;
		private int totalIndexes;
		private TaskModel taskModel;

		public ImportDatabaseCommand(TaskModel taskModel, Action<string> output)
		{
			this.output = output;
			this.taskModel = taskModel;
		}

		public override void Execute(object parameter)
		{
			var openFile = new OpenFileDialog
			               {
							   Filter = "Raven Dumps|*.ravendump;*.raven.dump",
			               };

			if (openFile.ShowDialog() != true)
				return;

			totalCount = 0;
			totalIndexes = 0;

			taskModel.TaskStatus = TaskStatus.Started;
			output(String.Format("Importing from {0}", openFile.File.Name));

			var sw = Stopwatch.StartNew();

			var stream = openFile.File.OpenRead();
			JsonTextReader jsonReader;
			if (TryGetJsonReader(stream, out jsonReader) == false)
			{
				stream.Dispose();
				return;
			}
			try
			{
				if (jsonReader.TokenType != JsonToken.StartObject)
					throw new InvalidOperationException("StartObject was expected");

				// should read indexes now
				if (jsonReader.Read() == false)
				{
					output("Invalid Json file specified!");
					stream.Dispose();
					return;
				}

				output(String.Format("Begin reading indexes"));

				if (jsonReader.TokenType != JsonToken.PropertyName)
					throw new InvalidOperationException("PropertyName was expected");

				if (Equals("Indexes", jsonReader.Value) == false)
					throw new InvalidOperationException("Indexes property was expected");

				if (jsonReader.Read() == false)
					return;

				if (jsonReader.TokenType != JsonToken.StartArray)
					throw new InvalidOperationException("StartArray was expected");

				// import Indexes
				WriteIndexes(jsonReader)
					.ContinueOnSuccess(() =>
					{
						output(String.Format("Done with reading indexes, total: {0}", totalIndexes));

						output(String.Format("Begin reading documents"));

						// should read documents now
						if (jsonReader.Read() == false)
						{
							output("There were no documents to load");
							stream.Dispose();
							return;
						}

						if (jsonReader.TokenType != JsonToken.PropertyName)
							throw new InvalidOperationException("PropertyName was expected");

						if (Equals("Docs", jsonReader.Value) == false)
							throw new InvalidOperationException("Docs property was expected");

						if (jsonReader.Read() == false)
						{
							output("There were no documents to load");
							stream.Dispose();
							return;
						}

						if (jsonReader.TokenType != JsonToken.StartArray)
							throw new InvalidOperationException("StartArray was expected");

						WriteDocuments(jsonReader)
							.ContinueOnSuccess(
								() =>
								output(String.Format("Imported {0:#,#;;0} documents in {1:#,#;;0} ms", totalCount, sw.ElapsedMilliseconds)))
							.Finally(() => taskModel.TaskStatus = TaskStatus.Ended);
					});
			}

			catch (Exception e)
			{
				taskModel.TaskStatus = TaskStatus.Ended;
				throw e;
			}
		}

		private Task WriteIndexes(JsonTextReader jsonReader)
		{
			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				var json = JToken.ReadFrom(jsonReader);
				var indexName = json.Value<string>("name");
				if (indexName.StartsWith("Temp/"))
				{
					continue;
				}

				var index = JsonConvert.DeserializeObject<IndexDefinition>(json.Value<JObject>("definition").ToString());

				totalIndexes++;

				output(String.Format("Importing index: {0}", indexName));

				return DatabaseCommands.PutIndexAsync(indexName, index, overwrite: true)
					.ContinueOnSuccess(() => WriteIndexes(jsonReader));
			}

			return Infrastructure.Execute.EmptyResult<object>();
		}

		private Task WriteDocuments(JsonTextReader jsonReader)
		{
			var batch = new List<RavenJObject>();
			while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
			{
				var document = RavenJToken.ReadFrom(jsonReader);
				batch.Add((RavenJObject) document);
				if (batch.Count >= BatchSize)
				{
					return FlushBatch(batch)
						.ContinueOnSuccess(() => WriteDocuments(jsonReader));
				}
			}
			return FlushBatch(batch);
		}

		private Stopwatch StopWatch = Stopwatch.StartNew();
		Task FlushBatch(List<RavenJObject> batch)
		{
			if (totalCount == 0)
			{
				StopWatch = Stopwatch.StartNew();
			}

			totalCount += batch.Count;

			var commands = (from doc in batch
			                let metadata = doc.Value<RavenJObject>("@metadata")
			                let removal = doc.Remove("@metadata")
			                select new PutCommandData
			                       {
			                       	Metadata = metadata,
			                       	Document = doc,
			                       	Key = metadata.Value<string>("@id"),
			                       }).ToArray();


			output(String.Format("Wrote {0} documents  in {1:#,#;;0} ms",
			                     batch.Count, StopWatch.ElapsedMilliseconds));
			StopWatch = Stopwatch.StartNew();

			return DatabaseCommands
				.BatchAsync(commands);
		}

		private bool TryGetJsonReader(FileStream stream, out JsonTextReader jsonReader)
		{
			// Try to read the stream compressed, otherwise continue uncompressed.
			try
			{
				var streamReader = new StreamReader(new GZipStream(stream, CompressionMode.Decompress));

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false)
				{
					output("Invalid json file found!");
					return false;
				}
			}
			catch (Exception)
			{
				output(String.Format("Import file did not use GZip compression, attempting to read as uncompressed."));

				stream.Seek(0, SeekOrigin.Begin);

				var streamReader = new StreamReader(stream);

				jsonReader = new JsonTextReader(streamReader);

				if (jsonReader.Read() == false)
				{
					output("Invalid json file found!");
					return false;
				}
			}
			return true;
		}
	}
}