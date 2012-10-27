using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Windows.Controls;
using Raven.Abstractions.Commands;
using Raven.Abstractions.Util;
using Raven.Client.Connection.Async;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;
using Kent.Boogaart.KBCsv;
using TaskStatus = Raven.Studio.Models.TaskStatus;

namespace Raven.Studio.Commands
{
	public class CsvImportCommand : Command
	{
		const int BatchSize = 512;
		private readonly Action<string> output;
		private readonly TaskModel taskModel;

		public CsvImportCommand(TaskModel taskModel, Action<string> output)
		{
			this.output = output;
			this.taskModel = taskModel;
		}

		public override void Execute(object parameter)
		{
			var openFile = new OpenFileDialog
							   {
								   Filter = "csv|*.csv"
							   };

			if (openFile.ShowDialog() != true)
				return;

			taskModel.TaskStatus = TaskStatus.Started;
			output(string.Format("Importing from {0}", openFile.File.Name));

			var streamReader = openFile.File.OpenText();

			var importImpl = new ImportImpl(streamReader, openFile.File.Name, taskModel, output,DatabaseCommands);
			importImpl.ImportAsync()
				.ContinueWith(task =>
				{
					importImpl.Dispose();
					return task;
				})
				.Unwrap()
				.Catch();
		}

		public class ImportImpl : IDisposable
		{
			private readonly TaskModel taskModel;
			private readonly Action<string> output;
			private readonly IAsyncDatabaseCommands databaseCommands;
			private readonly CsvReader csvReader;
			private readonly HeaderRecord header;
			private readonly string entity;
			private readonly Stopwatch sw;
			private IEnumerator<DataRecord> enumerator;
			private int totalCount;

			public ImportImpl(StreamReader reader, string file, TaskModel taskModel, Action<string> output, IAsyncDatabaseCommands databaseCommands)
			{
				this.taskModel = taskModel;
				this.output = output;
				this.databaseCommands = databaseCommands;
				csvReader = new CsvReader(reader);
				header = csvReader.ReadHeaderRecord();
				entity = Path.GetFileNameWithoutExtension(file);
				sw = Stopwatch.StartNew();

				enumerator = csvReader.DataRecords.GetEnumerator();
			}

			public Task ImportAsync()
			{
				var batch = new List<RavenJObject>();
				while (enumerator.MoveNext())
				{
					var record = enumerator.Current;
					var document = new RavenJObject();
					var id = Guid.NewGuid().ToString("N");
					foreach (var column in header.Values)
					{
						if (string.Compare("id", column, StringComparison.InvariantCultureIgnoreCase) == 0)
						{
							id = record[column];
						}
						else
						{
							document[column] = record[column];
						}
					}

					var metadata = new RavenJObject { { "Raven-Entity-Name", entity } };
					document.Add("@metadata", metadata);
					metadata.Add("@id", id);

					batch.Add(document);

					if (batch.Count >= BatchSize)
					{
						return FlushBatch(batch)
							.ContinueWith(t => t.IsCompleted ? ImportAsync() : t)
							.Unwrap();
					}
				}

				if(batch.Count > 0)
				{
					return FlushBatch(batch)
						.ContinueWith(t => t.IsCompleted ? ImportAsync() : t)
						.Unwrap();
				}

				output(String.Format("Imported {0:#,#;;0} documents in {1:#,#;;0} ms", totalCount, sw.ElapsedMilliseconds));
				taskModel.TaskStatus = TaskStatus.Ended;

				return new CompletedTask();
			}

			Task FlushBatch(List<RavenJObject> batch)
			{
				totalCount += batch.Count;
				var sw = Stopwatch.StartNew();
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
									 batch.Count, sw.ElapsedMilliseconds));

				return databaseCommands
					.BatchAsync(commands);
			}

			/// <summary>
			/// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
			/// </summary>
			public void Dispose()
			{
				csvReader.Close();
			}
		}
	}
}
