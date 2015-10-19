// -----------------------------------------------------------------------
//  <copyright file="DatabaseSmugglerFileDestination.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Globalization;
using System.IO;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Smuggler.Database.Impl.Streams;

namespace Raven.Smuggler.Database.Impl.Files
{
	public class DatabaseSmugglerFileDestination : DatabaseSmugglerStreamDestination
	{
		private readonly ILog _log = LogManager.GetCurrentClassLogger();

		private const string IncrementalExportStateFile = "IncrementalExport.state.json";

		private readonly string _path;

		public DatabaseSmugglerFileDestination(string path)
			: base(Stream.Null, leaveOpen: false)
		{
			_path = path;
		}

		public override void Initialize(DatabaseSmugglerOptions options)
		{
			var filePath = _path;
			if (options.Incremental)
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

			base.Initialize(options);
		}

		public override OperationState ModifyOperationState(DatabaseSmugglerOptions options, OperationState state)
		{
			var etagFileLocation = Path.Combine(_path, IncrementalExportStateFile);
			ReadLastEtagsFromFile(state, etagFileLocation);

			return state;
		}

		public void ReadLastEtagsFromFile(OperationState result, string etagFileLocation)
		{
			if (!File.Exists(etagFileLocation))
				return;

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
					return;
				}
				result.LastDocsEtag = Etag.Parse(ravenJObject.Value<string>("LastDocEtag"));
				result.LastDocDeleteEtag = Etag.Parse(ravenJObject.Value<string>("LastDocDeleteEtag") ?? Etag.Empty.ToString());
			}
		}
	}
}