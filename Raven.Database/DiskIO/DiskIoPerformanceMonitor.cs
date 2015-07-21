// -----------------------------------------------------------------------
//  <copyright file="DiskIoPerformanceMonitor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Microsoft.Diagnostics.Tracing.Parsers;
using Microsoft.Diagnostics.Tracing.Session;

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.DiskIO.Data;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Tenancy;

using Sparrow.Collections;

namespace Raven.Database.DiskIO
{
	public class DiskIoPerformanceMonitor : IDisposable
	{
		private readonly DatabasesLandlord databasesLandlord;

		private readonly ConcurrentSet<FileOperation> fileOperations = new ConcurrentSet<FileOperation>();

		private readonly FileSystemsLandlord fileSystemsLandlord;

		private readonly object locker = new object();

		private readonly ConcurrentDictionary<ulong, FileIOData> operations = new ConcurrentDictionary<ulong, FileIOData>();

		private readonly int processId;

		private readonly HashSet<ResourceToMonitor> resourcesToMonitor = new HashSet<ResourceToMonitor>();

		private readonly ConcurrentSet<IEventsTransport> serverTransports = new ConcurrentSet<IEventsTransport>();

		private readonly Timer timer;

		private TraceEventSession session;

		private bool started;

		public DiskIoPerformanceMonitor(DatabasesLandlord databasesLandlord, FileSystemsLandlord fileSystemsLandlord)
		{
			this.databasesLandlord = databasesLandlord;
			this.fileSystemsLandlord = fileSystemsLandlord;
			timer = databasesLandlord.SystemDatabase.TimerManager.NewTimer(ProcessOperations, TimeSpan.FromSeconds(0), TimeSpan.FromSeconds(5));

			processId = Process.GetCurrentProcess().Id;
		}

		public void Dispose()
		{
			DisposeSession();

			if (timer != null)
				timer.Dispose();
		}

		public void RegisterForAllResources(IEventsTransport transport)
		{
			serverTransports.Add(transport);
			transport.Disconnected += () =>
			{
				serverTransports.TryRemove(transport);
				StopMonitoringIfNecessary();
			};

			SubscribeToAllResources();
			StartMonitoringIfNecessary();
		}

		private void CreateSession()
		{
			session = new TraceEventSession("RavenIO");
			session.EnableKernelProvider(KernelTraceEventParser.Keywords.FileIOInit | KernelTraceEventParser.Keywords.FileIO | KernelTraceEventParser.Keywords.All);

			session.Source.Kernel.FileIORead += data =>
			{
				if (data.ProcessID != processId)
					return;

				var fileName = data.FileName;
				if (string.IsNullOrEmpty(fileName))
					return;

				if (resourcesToMonitor.Any(x => x.IsMatch(fileName)) == false)
					return;

				operations.AddOrUpdate(data.IrpPtr, new FileIOData(fileName, OperationType.Read, data.TimeStampRelativeMSec, data.IoSize), (_, file) =>
				{
					file.IoSizeInBytes += data.IoSize;
					return file;
				});
			};

			session.Source.Kernel.FileIOWrite += data =>
			{
				if (data.ProcessID != processId)
					return;

				var fileName = data.FileName;
				if (string.IsNullOrEmpty(fileName)) 
					return;

				if (resourcesToMonitor.Any(x => x.IsMatch(fileName)) == false)
					return;

				operations.AddOrUpdate(data.IrpPtr, new FileIOData(fileName, OperationType.Write, data.TimeStampRelativeMSec, data.IoSize), (_, file) =>
				{
					file.IoSizeInBytes += data.IoSize;
					return file;
				});
			};

			session.Source.Kernel.FileIOOperationEnd += data =>
			{
				if (data.ProcessID != processId)
					return;

				FileIOData value;
				if (operations.TryRemove(data.IrpPtr, out value) == false)
					return;

				var fileName = value.FileName;
				var ioSize = value.IoSizeInBytes;
				var durationInMilliseconds = data.TimeStampRelativeMSec - value.TimeStampRelativeInMilliseconds;
				var resourceInformation = GetResourceInformation(fileName);
				var operationType = value.OperationType;

				fileOperations.Add(new FileOperation(fileName, operationType, ioSize, durationInMilliseconds, resourceInformation));
			};
		}

		private void DisposeSession()
		{
			if (session == null)
				return;

			session.Dispose();
			session = null;
		}

		private ResourceInformation GetResourceInformation(string fileName)
		{
			foreach (var resourceToMonitor in resourcesToMonitor)
			{
				var path = resourceToMonitor.GetMatchingPath(fileName);
				if (path == null)
					continue;

				return new ResourceInformation { ResourceType = resourceToMonitor.ResourceType, ResourceName = resourceToMonitor.ResourceName, PathType = path.PathType };
			}

			throw new InvalidOperationException("Could not find matching resource.");
		}

		private void ProcessOperations(object state)
		{
			if (fileOperations.Count == 0)
				return;

			var list = fileOperations.ToList();
			fileOperations.Clear();

			var result = new Dictionary<ResourceType, Dictionary<string, Dictionary<PathType, object>>> { { ResourceType.Database, new Dictionary<string, Dictionary<PathType, object>>() }, { ResourceType.FileSystem, new Dictionary<string, Dictionary<PathType, object>>() } };

			foreach (var g in list.GroupBy(x => new { x.ResourceInformation.ResourceName, x.ResourceInformation.ResourceType, x.ResourceInformation.PathType }))
			{
				var writeDurationInMilliseconds = g.Where(x => x.OperationType == OperationType.Write).Sum(x => x.DurationInMilliseconds);
				var writeIoSize = g.Where(x => x.OperationType == OperationType.Write).Sum(x => x.IoSizeInBytes);

				var readDurationInMilliseconds = g.Where(x => x.OperationType == OperationType.Read).Sum(x => x.DurationInMilliseconds);
				var readIoSize = g.Where(x => x.OperationType == OperationType.Read).Sum(x => x.IoSizeInBytes);

				var resourceType = result[g.Key.ResourceType];
				var resource = resourceType.GetOrAdd(g.Key.ResourceName);

				resource.Add(g.Key.PathType, new
											 {
												 WriteDurationInMilliseconds = writeDurationInMilliseconds,
												 WriteIoSizeInBytes = writeIoSize,
												 ReadDurationInMilliseconds = readDurationInMilliseconds,
												 ReadIoSizeInBytes = readIoSize
											 });
			}

			try
			{
				foreach (var transport in serverTransports)
					transport.SendAsync(result);
			}
			catch (Exception)
			{
			}
		}

		private void StartMonitoringIfNecessary()
		{
			lock (locker)
			{
				if (started)
					return;

				if (serverTransports.Count <= 0)
					return;

				DisposeSession();
				CreateSession();

				Task.Factory.StartNew(() => session.Source.Process(), TaskCreationOptions.LongRunning);
				started = true;
			}
		}

		private void StopMonitoringIfNecessary()
		{
			lock (locker)
			{
				if (started == false)
					return;

				if (serverTransports.Count > 0)
					return;

				DisposeSession();
				started = false;
			}
		}

		private void SubscribeToAllResources()
		{
			databasesLandlord.ForAllDatabases(database => resourcesToMonitor.Add(new ResourceToMonitor { ResourceName = database.Name ?? Constants.SystemDatabase, ResourceType = ResourceType.Database, Paths = new[] { new PathInformation { Path = database.Configuration.IndexStoragePath, PathType = PathType.Index }, new PathInformation { Path = database.Configuration.DataDirectory, PathType = PathType.Data } } }));

			fileSystemsLandlord.ForAllFileSystems(fileSystem => resourcesToMonitor.Add(new ResourceToMonitor { ResourceName = fileSystem.Name, ResourceType = ResourceType.FileSystem, Paths = new[] { new PathInformation { Path = fileSystem.Configuration.FileSystem.IndexStoragePath, PathType = PathType.Index }, new PathInformation { Path = fileSystem.Configuration.FileSystem.DataDirectory, PathType = PathType.Data } } }));
		}

		private class FileIOData
		{
			public FileIOData(string fileName, OperationType operationType, double timeStampRelativeInMilliseconds, int ioSizeInBytes)
			{
				FileName = fileName;
				OperationType = operationType;
				TimeStampRelativeInMilliseconds = timeStampRelativeInMilliseconds;
				IoSizeInBytes = ioSizeInBytes;
			}

			public string FileName { get; private set; }

			public int IoSizeInBytes { get; set; }

			public OperationType OperationType { get; private set; }

			public double TimeStampRelativeInMilliseconds { get; private set; }
		}

		private class PathInformation
		{
			public string Path { get; set; }

			public PathType PathType { get; set; }
		}

		private class ResourceToMonitor : IComparable<ResourceToMonitor>
		{
			public PathInformation[] Paths { private get; set; }

			public string ResourceName { get; set; }

			public ResourceType ResourceType { get; set; }

			public int CompareTo(ResourceToMonitor other)
			{
				if (string.Equals(other.ResourceName, ResourceName, StringComparison.OrdinalIgnoreCase) && other.ResourceType == ResourceType)
					return 0;

				return -1;
			}

			public PathInformation GetMatchingPath(string path)
			{
				return Paths.FirstOrDefault(p => path.StartsWith(p.Path, StringComparison.OrdinalIgnoreCase));
			}

			public bool IsMatch(string path)
			{
				return Paths.Any(p => path.StartsWith(p.Path, StringComparison.OrdinalIgnoreCase));
			}
		}
	}
}