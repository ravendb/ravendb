using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.IO.Compression;
using System;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Exceptions;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Client.Counters;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

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

				exportOptions.ToFile = Path.Combine(exportOptions.ToFile, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-0", CultureInfo.InvariantCulture) + ".ravendb-incremental-dump");
				if (File.Exists(exportOptions.ToFile))
				{
					var counter = 1;
					while (true)
					{
						exportOptions.ToFile = Path.Combine(Path.GetDirectoryName(exportOptions.ToFile), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + "-" + counter + ".ravendb-incremental-dump");

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
						await ExportCounterData(exportOptions.From, jsonWriter);
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
			throw new NotImplementedException();
//			var counterStorageNames = await counterStore.Admin.GetCounterStoragesNamesAsync();
//			foreach (var storageName in counterStorageNames)
//			{
//				var counterStorageInfo = await counterStore.Admin.GetCounterStorageInfo(storageName);
//				jsonWriter.WriteStartObject();
//					
//					jsonWriter.WritePropertyName("CounterStorageName");
//					jsonWriter.WriteValue(counterStorageInfo.StorageName);
//					
//					jsonWriter.WritePropertyName("Counters");					
//					jsonWriter.WriteStartArray();
//						foreach (var counterInfo in counterStorageInfo.Counters)
//						{
//							jsonWriter.WriteStartObject();
//								jsonWriter.WritePropertyName("Group");
//								jsonWriter.WriteValue(counterInfo.Group);
//								
//								jsonWriter.WritePropertyName("Name");
//								jsonWriter.WriteValue(counterInfo.Name);
//
//								jsonWriter.WritePropertyName("Positive");
//								jsonWriter.WriteValue(counterInfo.PositiveCount);
//
//								jsonWriter.WritePropertyName("Negative");
//								jsonWriter.WriteValue(counterInfo.NegativeCount);
//								
//							jsonWriter.WriteEndObject();
//						}
//					jsonWriter.WriteEndArray();
//
//				jsonWriter.WriteEndObject();
//			}
		}

		public async Task ImportData(SmugglerImportOptions<CounterConnectionStringOptions> importOptions)
		{
			throw new NotImplementedException();
//			if (Options.Incremental == false)
//			{
//				Stream stream = importOptions.FromStream;
//				bool ownStream = false;
//				try
//				{
//					if (stream == null)
//					{
//						stream = File.OpenRead(importOptions.FromFile);
//						ownStream = true;
//					}
//					await ImportData(importOptions, stream);
//				}
//				finally
//				{
//					if (stream != null && ownStream)
//						stream.Dispose();
//				}
//				return;
//			}
//
//			var files = Directory.GetFiles(Path.GetFullPath(importOptions.FromFile))
//				.Where(file => ".ravendb-incremental-dump".Equals(Path.GetExtension(file), StringComparison.CurrentCultureIgnoreCase))
//				.OrderBy(File.GetLastWriteTimeUtc)
//				.ToArray();
//
//			if (files.Length == 0)
//				return;
//
//			var oldItemType = Options.OperateOnTypes;
//
//			Options.OperateOnTypes = Options.OperateOnTypes & ~(ItemType.Indexes | ItemType.Transformers);
//
//			for (var i = 0; i < files.Length - 1; i++)
//			{
//				using (var fileStream = File.OpenRead(Path.Combine(importOptions.FromFile, files[i])))
//				{
//					Operations.ShowProgress("Starting to import file: {0}", files[i]);
//					await ImportData(importOptions, fileStream);
//				}
//			}
//
//			Options.OperateOnTypes = oldItemType;
//
//			using (var fileStream = File.OpenRead(Path.Combine(importOptions.FromFile, files.Last())))
//			{
//				Operations.ShowProgress("Starting to import file: {0}", files.Last());
//				await ImportData(importOptions, fileStream);
//			}
		}

		public Task Between(SmugglerBetweenOptions<CounterConnectionStringOptions> betweenOptions)
		{
			throw new NotImplementedException();
		}
	}
}
