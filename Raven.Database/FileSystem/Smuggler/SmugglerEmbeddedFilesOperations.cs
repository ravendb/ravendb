// -----------------------------------------------------------------------
//  <copyright file="SmugglerEmbeddedDatabaseOperations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Smuggler
{
	public class SmugglerEmbeddedFilesOperations : ISmugglerFilesOperations
	{

		private readonly RavenFileSystem filesystem;

		public SmugglerEmbeddedFilesOperations(RavenFileSystem filesystem)
		{
			this.filesystem = filesystem;
		}

		public Action<string> Progress { get; set; }

		public SmugglerFilesOptions Options { get; private set; }

		public Task<FileSystemStats[]> GetStats()
		{
			var count = 0;
			filesystem.Storage.Batch(accessor =>
			{
				count = accessor.GetFileCount();
			});

			var stats = new FileSystemStats
			{
				Name = filesystem.Name,
				FileCount = count,
				Metrics = filesystem.CreateMetrics(),
				ActiveSyncs = filesystem.SynchronizationTask.Queue.Active.ToList(),
				PendingSyncs = filesystem.SynchronizationTask.Queue.Pending.ToList()
			};
			return new CompletedTask<FileSystemStats[]>(new [] {stats});
		}

		public Task<string> GetVersion(FilesConnectionStringOptions server)
		{
			return new CompletedTask<string>(DocumentDatabase.ProductVersion);
		}

		public LastFilesEtagsInfo FetchCurrentMaxEtags()
		{
			return new LastFilesEtagsInfo
			{
				LastFileEtag = null,
				LastDeletedFileEtag = null
			};
		}

		public Task<IAsyncEnumerator<FileHeader>> GetFiles(FilesConnectionStringOptions src, Etag lastEtag, int take)
		{
			ShowProgress("Streaming documents from {0}, batch size {1}", lastEtag, take);

			IEnumerable<FileHeader> enumerable = null;

			filesystem.Storage.Batch(accessor =>
			{
				enumerable = accessor.GetFilesAfter(lastEtag, take);
			});

			return new CompletedTask<IAsyncEnumerator<FileHeader>>(new AsyncEnumeratorBridge<FileHeader>(enumerable.GetEnumerator()));
		}

		public Task<Stream> DownloadFile(FileHeader file)
		{
			var name = file.FullPath;
			var readingStream = StorageStream.Reading(filesystem.Storage, name);
			return new CompletedTask<Stream>(readingStream);
		}

		public Task PutFiles(FileHeader file, Stream data, long dataSize)
		{
			//TODO: finish me - we need support for FilesActions in RavenFS?
			throw new NotImplementedException();
		}

		public void Initialize(SmugglerFilesOptions options)
		{
			Options = options;
		}

		public void Configure(SmugglerFilesOptions options)
		{
		}

		public void ShowProgress(string format, params object[] args)
		{
			if (Progress != null)
			{
				Progress(string.Format(format, args));
			}
		}

		public string CreateIncrementalKey()
		{
			throw new NotSupportedException("Copying between filesystems is currently not supported in embedded version.");
		}

		public Task<ExportFilesDestinations> GetIncrementalExportKey()
		{
			throw new NotSupportedException("Copying between filesystems is currently not supported in embedded version.");
		}

		public Task PutIncrementalExportKey(ExportFilesDestinations destinations)
		{
			throw new NotSupportedException("Copying between filesystems is currently not supported in embedded version.");
		}

		public RavenJObject StripReplicationInformationFromMetadata(RavenJObject metadata)
		{
			if (metadata != null)
			{
				metadata.Remove(SynchronizationConstants.RavenSynchronizationHistory);
				metadata.Remove(SynchronizationConstants.RavenSynchronizationSource);
				metadata.Remove(SynchronizationConstants.RavenSynchronizationVersion);
			}

			return metadata;
		}

		public RavenJObject DisableVersioning(RavenJObject metadata)
		{
			if (metadata != null)
			{
				metadata.Add(Constants.RavenIgnoreVersioning, true);
			}

			return metadata;
		}
	}
}