using System.Net;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Client.Counters;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

using System;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading.Tasks;

namespace Raven.Database.Smuggler
{
	public class SmugglerCounterApi : ISmugglerApi<CounterConnectionStringOptions, SmugglerCounterOptions, CounterOperationState>
	{
		private readonly ICounterStore counterStore;
		private const string IncrementalExportStateFile = "IncrementalExport.state.json";

		public SmugglerCounterApi(ICounterStore counterStore)
		{
			if (counterStore == null) throw new ArgumentNullException("counterStore");
			this.counterStore = counterStore;
			Options = new SmugglerCounterOptions();
		}

		public SmugglerCounterOptions Options { get; private set; }

		public async Task<CounterOperationState> ExportData(SmugglerExportOptions<CounterConnectionStringOptions> exportOptions)
		{
			var result = new CounterOperationState();
			if (Options.Incremental)
			{
				if (Directory.Exists(exportOptions.ToFile) == false)
				{
					if (File.Exists(exportOptions.ToFile))
						exportOptions.ToFile = Path.GetDirectoryName(exportOptions.ToFile) ?? exportOptions.ToFile;
					else
						Directory.CreateDirectory(exportOptions.ToFile);
				}

				exportOptions.ToFile = Path.Combine(exportOptions.ToFile, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-0", CultureInfo.InvariantCulture) + ".counter-incremental-dump");
				if (File.Exists(exportOptions.ToFile))
				{
					var counter = 1;
					while (true)
					{
						exportOptions.ToFile = Path.Combine(Path.GetDirectoryName(exportOptions.ToFile), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + "-" + counter + ".counter-incremental-dump");

						if (File.Exists(exportOptions.ToFile) == false)
							break;
						counter++;
					}
				}
			}

			SmugglerExportException lastException = null;

			bool ownedStream = exportOptions.ToStream == null;
			var stream = exportOptions.ToStream ?? File.Create(exportOptions.ToFile);

			try
			{
				using (var gZipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
				using (var streamWriter = new StreamWriter(gZipStream))
				{
					var jsonWriter = new JsonTextWriter(streamWriter)
					{
						Formatting = Formatting.Indented
					};
					jsonWriter.WriteStartObject();			
					jsonWriter.WritePropertyName("Counters");
					jsonWriter.WriteStartArray();

					try
					{
						await ExportCounterData(exportOptions.From, jsonWriter).ConfigureAwait(false);
					}
					catch (SmugglerExportException e)
					{
						Debug.Assert(e.Data.Keys.Cast<string>().Contains("LastEtag"));
						result.LastWrittenEtag = (long)e.Data["LastEtag"];
						lastException = e;
					}

					jsonWriter.WriteEndArray();
					jsonWriter.WriteEndObject();
					streamWriter.Flush();
				}

				if (Options.Incremental)
					WriteLastEtagsToFile(result, result.CounterId, exportOptions.ToFile);

				if (lastException != null)
					throw lastException;

				return result;
			}
			finally
			{
				if (ownedStream && stream != null)
					stream.Dispose();
			}
		}

		public static void WriteLastEtagsToFile(CounterOperationState result, string counterId, string exportFilename)
		{
			using (var streamWriter = new StreamWriter(File.Create(IncrementalExportStateFile)))
			{
				new RavenJObject
					{
						{ "ExportFilename", exportFilename },
						{ "LastWrittenEtag", result.LastWrittenEtag.ToString(CultureInfo.InvariantCulture) },
					}.WriteTo(new JsonTextWriter(streamWriter));
				streamWriter.Flush();
			}
		}

		private async Task ExportCounterData(CounterConnectionStringOptions @from, JsonTextWriter jsonWriter)
		{
			var counterStorageNames = await counterStore.Admin.GetCounterStoragesNamesAsync();
			foreach (var storageName in counterStorageNames)
			{
				var counterStorageInfo = await counterStore.Admin.GetCounterStorageSummary(storageName);
					
				jsonWriter.WriteStartArray();
					foreach (var counterInfo in counterStorageInfo)
					{
						jsonWriter.WriteStartObject();
							jsonWriter.WritePropertyName("Group");
							jsonWriter.WriteValue(counterInfo.Group);
							
							jsonWriter.WritePropertyName("Name");
							jsonWriter.WriteValue(counterInfo.CounterName);
							jsonWriter.WritePropertyName("Positive");
							jsonWriter.WriteValue(counterInfo.Increments);

							jsonWriter.WritePropertyName("Negative");
							jsonWriter.WriteValue(counterInfo.Decrements);
								
						jsonWriter.WriteEndObject();
					}
				jsonWriter.WriteEndArray();

			}
		}

		//assumes that the caller has responsibility to handle data stream disposal ("stream" parameter)
		private async Task ImportData(CounterConnectionStringOptions connectionString, Stream stream)
		{
			CountingStream sizeStream;
			JsonTextReader jsonReader;
			if (SmugglerHelper.TryGetJsonReaderForStream(stream, out jsonReader, out sizeStream) == false)
			{
				throw new InvalidOperationException("Failed to get reader for the data stream.");
			}

			if(jsonReader.TokenType != JsonToken.StartObject)
				throw new InvalidDataException("StartObject was expected");

			ICounterStore store = null;

			try
			{
				if (jsonReader.Read() == false && jsonReader.TokenType != JsonToken.StartArray)
					throw new InvalidDataException("StartArray was expected");

				store = new CounterStore
				{
					Url = connectionString.Url,
					Name = connectionString.CounterStoreId,
					Credentials = new OperationCredentials(connectionString.ApiKey, connectionString.Credentials)
				};
				store.Initialize();

				while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
				{
					if (jsonReader.TokenType == JsonToken.StartObject)
					{
						var counterInfo = (RavenJObject) RavenJToken.ReadFrom(jsonReader);

						var delta = Math.Abs(counterInfo.Value<long>("Positive")) - Math.Abs(counterInfo.Value<long>("Negative"));
						store.Batch.ScheduleChange(counterInfo.Value<string>("Group"), counterInfo.Value<string>("Name"), delta);
					}
				}

				await store.Batch.FlushAsync().ConfigureAwait(false);
			}
			finally
			{
				if(store != null)
					store.Dispose();				
			}
		}

		public async Task ImportData(SmugglerImportOptions<CounterConnectionStringOptions> importOptions)
		{
			if (String.IsNullOrWhiteSpace(importOptions.FromFile) && importOptions.FromStream == null)
				throw new ArgumentException("Missing from paramter from import options - be sure to define either FromFile or FromStream property");

			if(importOptions.To == null)
				throw new ArgumentException("Missing To parameter from importOptions - do not know where to import to.");

			if (String.IsNullOrWhiteSpace(importOptions.To.Url))
				throw new ArgumentException("Missing Url of the RavenDB server - do not know where to import to");

			if(String.IsNullOrWhiteSpace(importOptions.To.CounterStoreId))
				throw new ArgumentException("Missing Id of the Counter Store - do not know where to import to");

			if (Options.Incremental == false)
			{
				var stream = importOptions.FromStream;
				var ownStream = false;
				try
				{
					if (stream == null)
					{
						stream = File.OpenRead(importOptions.FromFile);
						ownStream = true;
					}

					await ImportData(importOptions.To, stream).ConfigureAwait(false);
				}
				finally
				{
					if (stream != null && ownStream)
						stream.Dispose();
				}
			}
			else
			{
				var files = Directory.GetFiles(Path.GetFullPath(importOptions.FromFile))
					.Where(file => ".ravendb-incremental-dump".Equals(Path.GetExtension(file), StringComparison.CurrentCultureIgnoreCase))
					.OrderBy(File.GetLastWriteTimeUtc)
					.ToArray();

				if (files.Length == 0)
					return;

				for (var i = 0; i < files.Length - 1; i++)
				{
					using (var fileStream = File.OpenRead(Path.Combine(importOptions.FromFile, files[i])))
					{
						//Operations.ShowProgress("Starting to import file: {0}", files[i]);
						await ImportData(importOptions.To, fileStream);
					}
				}

				using (var fileStream = File.OpenRead(Path.Combine(importOptions.FromFile, files.Last())))
				{
					//Operations.ShowProgress("Starting to import file: {0}", files.Last());
					await ImportData(importOptions.To, fileStream).ConfigureAwait(false);
				}
			}
		}

		public Task Between(SmugglerBetweenOptions<CounterConnectionStringOptions> betweenOptions)
		{
			throw new NotImplementedException();
		}
	}
}
