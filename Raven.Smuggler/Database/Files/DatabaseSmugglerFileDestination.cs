// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerFileDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.Database;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Smuggler.Database.Streams;

namespace Raven.Smuggler.Database.Files
{
	public class DatabaseSmugglerFileDestination : DatabaseSmugglerStreamDestination
	{
		private readonly ILog _log = LogManager.GetCurrentClassLogger();

		private const string IncrementalExportStateFile = "IncrementalExport.state.json";

		private readonly string _path;

		private readonly DatabaseSmugglerFileDestinationOptions _options;

		public DatabaseSmugglerFileDestination(string path, DatabaseSmugglerFileDestinationOptions options = null)
			: base(Stream.Null, leaveOpen: false)
		{
			_path = path;
			_options = options ?? new DatabaseSmugglerFileDestinationOptions();
		}

		public override bool SupportsOperationState => true;

		public override async Task InitializeAsync(DatabaseSmugglerOptions options, DatabaseSmugglerNotifications notifications, CancellationToken cancellationToken)
		{
			var filePath = _path;
			if (_options.Incremental)
			{
				if (Directory.Exists(_path) == false)
				{
					if (File.Exists(_path))
						filePath = Path.GetDirectoryName(_path) ?? _path;
					else
						Directory.CreateDirectory(_path);
				}

				filePath = Path.Combine(filePath, SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm-0", CultureInfo.InvariantCulture) + ".ravendb-incremental-dump");
				if (File.Exists(filePath))
				{
					var counter = 1;
					while (true)
					{
						filePath = Path.Combine(Path.GetDirectoryName(filePath), SystemTime.UtcNow.ToString("yyyy-MM-dd-HH-mm", CultureInfo.InvariantCulture) + "-" + counter + ".ravendb-incremental-dump");

						if (File.Exists(filePath) == false)
							break;
						counter++;
					}
				}
			}

			_stream = File.Create(filePath);

			await base.InitializeAsync(options, notifications, cancellationToken).ConfigureAwait(false);
		}

		public override Task<DatabaseSmugglerOperationState> LoadOperationStateAsync(DatabaseSmugglerOptions options, CancellationToken cancellationToken)
		{
			var etagFileLocation = Path.Combine(_path, IncrementalExportStateFile);

			return new CompletedTask<DatabaseSmugglerOperationState>(ReadLastEtagsFromFile(etagFileLocation));
		}

		public override Task SaveOperationStateAsync(DatabaseSmugglerOptions options, DatabaseSmugglerOperationState state, CancellationToken cancellationToken)
		{
			if (_options.Incremental)
			{
				var etagFileLocation = Path.Combine(_path, IncrementalExportStateFile);

				WriteLastEtagsToFile(state, etagFileLocation);
			}

			return new CompletedTask();
		}

		private static void WriteLastEtagsToFile(DatabaseSmugglerOperationState state, string etagFileLocation)
		{
			using (var streamWriter = new StreamWriter(File.Create(etagFileLocation)))
			{
				new RavenJObject
					{
						{"LastDocEtag", state.LastDocsEtag.ToString()},
						{"LastDocDeleteEtag", state.LastDocDeleteEtag.ToString()},
					}.WriteTo(new JsonTextWriter(streamWriter));
				streamWriter.Flush();
			}
		}

		private DatabaseSmugglerOperationState ReadLastEtagsFromFile(string etagFileLocation)
		{
			if (File.Exists(etagFileLocation) == false)
				return null;

			using (var streamReader = new StreamReader(new FileStream(etagFileLocation, FileMode.Open)))
			using (var jsonReader = new JsonTextReader(streamReader))
			{
				RavenJObject ravenJObject;
				try
				{
					ravenJObject = RavenJObject.Load(jsonReader);
				}
				catch (Exception e)
				{
					_log.WarnException("Could not parse etag document from file : " + etagFileLocation + ", ignoring, will start from scratch", e);
					return null;
				}

				return new DatabaseSmugglerOperationState
				{
					LastDocsEtag = Etag.Parse(ravenJObject.Value<string>("LastDocEtag")),
					LastDocDeleteEtag = Etag.Parse(ravenJObject.Value<string>("LastDocDeleteEtag") ?? Etag.Empty.ToString())
				};
			}
		}
	}
}