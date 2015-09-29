using System.Collections.Generic;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Client.Counters;
using Raven.Database.Counters;
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
		private const string IncrementalExportStateFile = "IncrementalExport.state.json";
		private const string CounterIncrementalDump = ".counter-incremental-dump";


		private Action<string> showProgress;

		public SmugglerCounterApi(Action<string> showProgress = null)
		{
			ShowProgress = showProgress;
			Options = new SmugglerCounterOptions();
		}

		public SmugglerCounterOptions Options { get; set; }

		public Action<string> ShowProgress
		{
			get
			{
				return showProgress;
			}
			set
			{
				if (value == null)
					showProgress = msg => { };
				else
					showProgress = value;
			}

		}

		public CancellationToken CancellationToken
		{
			get
			{
				return (Options == null || Options.CancelToken == null) ? CancellationToken.None : Options.CancelToken.Token;
			}
		}

		/// <summary>
		/// Export counter data to specified destination (a file or a stream)
		/// </summary>
		/// <param name="exportOptions">options to specify the source and destination of the export</param>
		/// <exception cref="UnauthorizedAccessException">The caller does not have the required permission.-or- specified a file that is read-only. </exception>
		/// <exception cref="DirectoryNotFoundException">The specified path is invalid (for example, it is on an unmapped drive). </exception>
		/// <exception cref="IOException">An I/O error occurred while creating the file. </exception>
		/// <exception cref="SmugglerExportException">Encapsulates exception that happens when actually exporting data. See InnerException for details.</exception>
		public async Task<CounterOperationState> ExportData(SmugglerExportOptions<CounterConnectionStringOptions> exportOptions)
		{
			if(exportOptions.From == null)
				throw new ArgumentNullException("exportOptions.From");

			if(String.IsNullOrWhiteSpace(exportOptions.ToFile) && exportOptions.ToStream == null)
				throw new ArgumentException("ToFile or ToStream property in options must be non-null");

			var result = new CounterOperationState();
			var exportFolder = String.Empty;
			if (Options.Incremental)
			{
				ShowProgress("Starting incremental export..");
				exportFolder = CalculateExportFile(exportOptions, exportFolder);
			}
			else
			{
				ShowProgress("Starting full export...");
			}

			SmugglerExportException lastException = null;

			var ownedStream = exportOptions.ToStream == null;
			var stream = exportOptions.ToStream ?? File.Create(exportOptions.ToFile);
			if (ownedStream)
				ShowProgress("Export to dump file " + exportOptions.ToFile);
			try
			{
				using (var counterStore = new CounterStore
				{
					Url = exportOptions.From.Url,
					Name = exportOptions.From.CounterStoreId,
					Credentials = new OperationCredentials(exportOptions.From.ApiKey, exportOptions.From.Credentials)
				})
				using (var gZipStream = new GZipStream(stream, CompressionMode.Compress, leaveOpen: true))
				using (var streamWriter = new StreamWriter(gZipStream))
				{
					counterStore.Initialize();
					var jsonWriter = new JsonTextWriter(streamWriter)
					{
						Formatting = Formatting.Indented
					};
					jsonWriter.WriteStartObject();
					jsonWriter.WritePropertyName(Options.Incremental ? "CountersDeltas" : "CounterSnapshots"); //also for human readability
					jsonWriter.WriteStartArray();

					try
					{
						if (Options.Incremental)
							await ExportIncrementalData(counterStore, exportFolder, jsonWriter).WithCancellation(CancellationToken).ConfigureAwait(false);
						else
							await ExportFullData(counterStore, jsonWriter).WithCancellation(CancellationToken).ConfigureAwait(false);
					}
					catch (SmugglerExportException e)
					{
						Debug.Assert(e.Data.Keys.Cast<string>().Contains("LastEtag"));
						result.LastWrittenEtag = (long) e.Data["LastEtag"];
						lastException = e;
						var operation = Options.Incremental ? "Incremental" : "Full";
						ShowProgress(String.Format("{0} Export failed. {1}", operation, e));
					}

					jsonWriter.WriteEndArray();
					jsonWriter.WriteEndObject();
					streamWriter.Flush();
				}

				if (lastException != null)
					throw lastException;
				return result;
			}
			finally
			{
				if (ownedStream && stream != null)
				{
					stream.Flush();
					stream.Dispose();
					ShowProgress("Finished export and closed file...");
				}
				else
				{
					ShowProgress("Finished export...");
				}
			}
		}

		private static string CalculateExportFile(SmugglerExportOptions<CounterConnectionStringOptions> exportOptions, string exportFolder)
		{
			if (Directory.Exists(exportOptions.ToFile) == false)
			{
				if (File.Exists(exportOptions.ToFile))
					exportOptions.ToFile = Path.GetDirectoryName(exportOptions.ToFile) ?? exportOptions.ToFile;
				else
					Directory.CreateDirectory(exportOptions.ToFile);
			}
			exportFolder = exportOptions.ToFile;

			exportOptions.ToFile = Path.Combine(exportOptions.ToFile, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-0", CultureInfo.InvariantCulture) + CounterIncrementalDump);
			if (File.Exists(exportOptions.ToFile))
			{
				var counter = 1;
				while (true)
				{
					exportOptions.ToFile = Path.Combine(Path.GetDirectoryName(exportOptions.ToFile), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + "-" + counter + CounterIncrementalDump);

					if (File.Exists(exportOptions.ToFile) == false)
						break;
					counter++;
				}
			}
			return exportFolder;
		}

		private async Task ExportIncrementalData(ICounterStore counterStore, string exportFilename, JsonTextWriter jsonWriter)
		{
			var lastEtag = ReadLastEtagFromStateFile(exportFilename);
			var counterDeltas = (await GetCounterStatesSinceEtag(counterStore, lastEtag).WithCancellation(CancellationToken).ConfigureAwait(false)).ToList();

			ShowProgress("Incremental export -> starting from etag : " + lastEtag);

			foreach (var delta in counterDeltas)
			{
				ShowProgress(String.Format("Exporting counter {0} - {1}", delta.GroupName, delta.CounterName));

				jsonWriter.WriteStartObject();
				jsonWriter.WritePropertyName("CounterName");
				jsonWriter.WriteValue(delta.CounterName);

				jsonWriter.WritePropertyName("GroupName");
				jsonWriter.WriteValue(delta.GroupName);

				jsonWriter.WritePropertyName("Sign");
				jsonWriter.WriteValue(delta.Sign);

				jsonWriter.WritePropertyName("Value");
				jsonWriter.WriteValue(delta.Value);
				jsonWriter.WriteEndObject();
			}

			if (counterDeltas.Count > 0)
			{
				var etag = counterDeltas.Max(x => x.Etag);
				ShowProgress("Incremental export -> finished export, last exported etag : " + etag);
				WriteLastEtagToStateFile(exportFilename, etag);
			}
		}

		private async Task<IEnumerable<CounterState>> GetCounterStatesSinceEtag(ICounterStore counterStore, long etag)
		{
			var deltas = new List<CounterState>();
			do
			{
				var deltasFromRequest = await counterStore.Advanced.GetCounterStatesSinceEtag(etag, token: CancellationToken).ConfigureAwait(false);
				if (deltasFromRequest.Count == 0)
					break;

				etag = deltasFromRequest.Max(x => x.Etag);
				deltas.AddRange(deltasFromRequest);
			} while (true);

			return deltas;
		}

		private static long ReadLastEtagFromStateFile(string exportFilename)
		{
			var exportStateFilePath = Path.Combine(exportFilename, IncrementalExportStateFile);
			if (File.Exists(exportStateFilePath) == false)
				return 0;

			using (var streamReader = new StreamReader(File.Open(exportStateFilePath,FileMode.OpenOrCreate)))
			{
				var jsonObject = RavenJToken.ReadFrom(new JsonTextReader(streamReader));
				long lastWrittenEtag;
				var lastWrittenEtagString = jsonObject.Value<string>("LastWrittenEtag");
				if(Int64.TryParse(lastWrittenEtagString,out lastWrittenEtag) == false)
					throw new InvalidDataException("Failed to parse incremental export status file. Found in file : " + lastWrittenEtagString);

				return lastWrittenEtag;
			}
		}

		private static void WriteLastEtagToStateFile(string exportFilename, long lastEtag)
		{
			var exportStateFile = Path.Combine(exportFilename,IncrementalExportStateFile);
			using (var streamWriter = new StreamWriter(File.Open(exportStateFile, FileMode.Create)))
			{
				new RavenJObject
					{
						{ "LastWrittenEtag", lastEtag.ToString(CultureInfo.InvariantCulture) },
					}.WriteTo(new JsonTextWriter(streamWriter));
				streamWriter.Flush();
			}
		}

		private async Task ExportFullData(ICounterStore counterStore, JsonTextWriter jsonWriter)
		{
			ShowProgress("Starting full export...");
			ShowProgress("Exporting from counter storage " + counterStore.Name);
			var counterStorageInfo = await counterStore.Admin.GetCounterStorageSummary(counterStore.Name, CancellationToken).WithCancellation(CancellationToken).ConfigureAwait(false);

			jsonWriter.WriteStartArray();
			foreach (var counterInfo in counterStorageInfo)
			{
				ShowProgress(String.Format("Exporting counter {0} - {1}", counterInfo.GroupName, counterInfo.CounterName));
				jsonWriter.WriteStartObject();
				jsonWriter.WritePropertyName("Group");
				jsonWriter.WriteValue(counterInfo.GroupName);

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

		//assumes that the caller has responsibility to handle data stream disposal ("stream" parameter)
		private async Task ImportFullData(CounterConnectionStringOptions connectionString, Stream stream)
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
				store.Initialize(true);

				ShowProgress(String.Format("Initialized connection to counter store (name = {0})",store.Name));
				var existingCounterGroupsAndNames = await store.Admin.GetCounterStorageNameAndGroups(token: CancellationToken)
																	 .WithCancellation(CancellationToken)
																	 .ConfigureAwait(false);

				while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
				{
					if (jsonReader.TokenType != JsonToken.StartObject) 
						continue;

					var counterInfo = (RavenJObject) RavenJToken.ReadFrom(jsonReader);

					var delta = Math.Abs(counterInfo.Value<long>("Positive")) - Math.Abs(counterInfo.Value<long>("Negative"));
					var groupName = counterInfo.Value<string>("Group");
					var counterName = counterInfo.Value<string>("Name");

					if (existingCounterGroupsAndNames.Any(x => x.Group == groupName && x.Name == counterName))
					{
						ShowProgress(String.Format("Counter {0} - {1} is already there. Reset is performed", groupName, counterName));
						await store.ResetAsync(groupName, counterName, CancellationToken).ConfigureAwait(false); //since it is a full import, the values are overwritten
					}

					ShowProgress(String.Format("Importing counter {0} - {1}",groupName,counterName));
					store.Batch.ScheduleChange(groupName, counterName, delta);
				}

				ShowProgress("Finished import...");
				await store.Batch.FlushAsync().WithCancellation(CancellationToken).ConfigureAwait(false);
			}
			finally
			{
				if(store != null)
					store.Dispose();				
			}
		}

		
		/// <summary>
		/// Import counter data from a dump file
		/// </summary>
		/// <param name="importOptions">options that specify the source and destination of the data</param>
		/// <exception cref="ArgumentException">FromXXXX, To, Url and CounterStoreId parameters must be present in the import options</exception>
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
						ShowProgress(String.Format("Starting full import from file : {0}", importOptions.FromFile));
						ownStream = true;
					}
					else
					{
						ShowProgress("Starting full import from stream");
					}

					await ImportFullData(importOptions.To, stream).WithCancellation(CancellationToken).ConfigureAwait(false);
				}
				finally
				{
					if (stream != null && ownStream)
						stream.Dispose();
				}
			}
			else
			{
				var dumpFilePath = Path.GetFullPath(importOptions.FromFile);
				ShowProgress("Enumerating incremental dump files at " + dumpFilePath);
				var files = Directory.GetFiles(dumpFilePath)
					.Where(file => CounterIncrementalDump.Equals(Path.GetExtension(file), StringComparison.CurrentCultureIgnoreCase))
					.OrderBy(File.GetLastWriteTimeUtc)
					.ToArray();

				if (files.Length == 0)
					return;

				foreach(var file in files)
				{
					using (var fileStream = File.OpenRead(Path.Combine(importOptions.FromFile, file)))
					{
						ShowProgress(String.Format("Starting incremental import from file: {0}", file));
						await ImportIncrementalData(importOptions.To, fileStream).WithCancellation(CancellationToken).ConfigureAwait(false);
					}
				}
			}
		}

		private async Task ImportIncrementalData(CounterConnectionStringOptions connectionString, Stream stream)
		{
			CountingStream sizeStream;
			JsonTextReader jsonReader;
			if (SmugglerHelper.TryGetJsonReaderForStream(stream, out jsonReader, out sizeStream) == false)
				throw new InvalidOperationException("Failed to get reader for the data stream.");

			if (jsonReader.TokenType != JsonToken.StartObject)
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
				store.Initialize(true);
				ShowProgress(String.Format("Initialized connection to counter store (name = {0})", store.Name));

				while (jsonReader.Read() && jsonReader.TokenType != JsonToken.EndArray)
				{
					if (jsonReader.TokenType != JsonToken.StartObject) 
						continue;

					var counterDelta = RavenJToken.ReadFrom(jsonReader).ToObject<CounterState>();
					ShowProgress(String.Format("Importing counter {0} - {1}", counterDelta.GroupName, counterDelta.CounterName));
					if (counterDelta.Sign == ValueSign.Negative)
						counterDelta.Value = -counterDelta.Value;
					store.Batch.ScheduleChange(counterDelta.GroupName, counterDelta.CounterName, counterDelta.Value);
				}

				ShowProgress("Finished import of the current file.");
				await store.Batch.FlushAsync().WithCancellation(CancellationToken).ConfigureAwait(false);
			}
			finally
			{
				if (store != null)
					store.Dispose();
			}
		}

		public async Task Between(SmugglerBetweenOptions<CounterConnectionStringOptions> betweenOptions)
		{
			if (betweenOptions.ReportProgress == null)
				betweenOptions.ReportProgress = msg => { };
			using (var source = new CounterStore
			{
				Url = betweenOptions.From.Url,
				Name = betweenOptions.From.CounterStoreId,
				Credentials = new OperationCredentials(betweenOptions.From.ApiKey, betweenOptions.From.Credentials)
			})
			using (var target = new CounterStore
			{
				Url = betweenOptions.To.Url,
				Name = betweenOptions.To.CounterStoreId,
				Credentials = new OperationCredentials(betweenOptions.To.ApiKey, betweenOptions.To.Credentials)
			})
			{
				source.Initialize(true);
				ShowProgress(String.Format("Initialized connection to source counter store (name = {0})", source.Name));
				target.Initialize(true);
				ShowProgress(String.Format("Initialized connection to target counter store (name = {0})", target.Name));

				var existingCounterGroupsAndNames = await target.Admin.GetCounterStorageNameAndGroups(token: CancellationToken).ConfigureAwait(false);
				var counterSummaries = await source.Admin.GetCounterStorageSummary(token: CancellationToken).ConfigureAwait(false);
				ShowProgress(String.Format("Fetched counter data from source (there is data about {0} counters)",counterSummaries.Length));

				foreach (var summary in counterSummaries)
				{
					if (existingCounterGroupsAndNames.Any(x => x.Group == summary.GroupName && x.Name == summary.CounterName))
					{
						ShowProgress(String.Format("Counter {0} - {1} is already there. Reset is performed", summary.GroupName, summary.CounterName));
						await target.ResetAsync(summary.GroupName, summary.CounterName, CancellationToken)
							.WithCancellation(CancellationToken)
							.ConfigureAwait(false); //since it is a full import, the values are overwritten
					}

					ShowProgress(String.Format("Importing counter {0} - {1}", summary.GroupName, summary.CounterName));
					target.Batch.ScheduleChange(summary.GroupName, summary.CounterName, summary.Total);
				}

				ShowProgress("Finished import...");
				await target.Batch.FlushAsync().WithCancellation(CancellationToken).ConfigureAwait(false);
			}
		}
	}
}
