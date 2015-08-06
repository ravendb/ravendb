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

using Microsoft.Diagnostics.Tracing.Etlx;
using Microsoft.Diagnostics.Tracing.Parsers;
using Microsoft.Diagnostics.Tracing.Session;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client;
using Raven.Client.Document;
using Raven.Json.Linq;
using Raven.Monitor.IO.Data;

namespace Raven.Monitor.IO
{
	internal class DiskIoPerformanceMonitor : IDisposable
	{
		private readonly BlockingCollection<FileOperation> fileOperations = new BlockingCollection<FileOperation>();

		private readonly ConcurrentDictionary<ulong, FileIoData> operations = new ConcurrentDictionary<ulong, FileIoData>();

		private readonly Timer timer;

		private readonly object locker = new object();

		private readonly IDocumentStore store;

		private readonly DiskIoPerformanceRun run = new DiskIoPerformanceRun();

		private TraceEventSession session;

		private Task processingTask;

		private bool isRunning;

		public DiskIoPerformanceMonitor(MonitorOptions options)
		{
			var process = Process.GetProcessById(options.ProcessId);

			store = new DocumentStore { Url = options.ServerUrl }.Initialize();
			var resourcesToMonitor = GetResourcesToMonitor();

			run.StartTime = SystemTime.UtcNow;
			run.ProcessId = options.ProcessId;
			run.ProcessName = process.ProcessName;
			run.DurationInMinutes = options.IoOptions.DurationInMinutes;
			run.DisplayName = GenerateDisplayName(run);

			timer = new Timer(_ => Stop(), null, TimeSpan.FromMinutes(options.IoOptions.DurationInMinutes), TimeSpan.FromMilliseconds(-1));

			session = new TraceEventSession(KernelTraceEventParser.KernelSessionName);
			session.EnableKernelProvider(KernelTraceEventParser.Keywords.FileIOInit | KernelTraceEventParser.Keywords.FileIO);

			session.Source.Kernel.FileIORead += data =>
			{
				if (data.ProcessID != options.ProcessId)
					return;

				var fileName = data.FileName;
				if (string.IsNullOrEmpty(fileName))
					return;

				if (resourcesToMonitor.Any(x => x.IsMatch(fileName)) == false)
					return;

				operations.AddOrUpdate(data.IrpPtr, new FileIoData(fileName, OperationType.Read, data.TimeStampRelativeMSec, data.IoSize), (_, d) =>
				{
					d.IoSizeInBytes += data.IoSize;
					return d;
				});
			};

			session.Source.Kernel.FileIOWrite += data =>
			{
				if (data.ProcessID != options.ProcessId)
					return;

				var fileName = data.FileName;
				if (string.IsNullOrEmpty(fileName))
					return;

				if (resourcesToMonitor.Any(x => x.IsMatch(fileName)) == false)
					return;

				operations.AddOrUpdate(data.IrpPtr, new FileIoData(fileName, OperationType.Write, data.TimeStampRelativeMSec, data.IoSize), (_, d) =>
				{
					d.IoSizeInBytes += data.IoSize;
					return d;
				});
			};

			session.Source.Kernel.FileIOOperationEnd += data =>
			{
				//NOTE: we can't compare data.ProcessID with options.ProcessId, because data.ProcessID is always -1 on Windows 7 (however it works on Win 8).

				FileIoData value;
				if (operations.TryRemove(data.IrpPtr, out value) == false)
					return;

				var fileName = value.FileName;
				var ioSize = value.IoSizeInBytes;
				var durationInMilliseconds = data.TimeStampRelativeMSec - value.TimeStampRelativeInMilliseconds;
				var resourceInformation = GetResourceInformation(fileName, resourcesToMonitor);
				var operationType = value.OperationType;
				var utcTimeStamp = data.TimeStamp.ToUniversalTime();

				var endTimeStamp = new DateTime(utcTimeStamp.Year, utcTimeStamp.Month, utcTimeStamp.Day, utcTimeStamp.Hour, utcTimeStamp.Minute, utcTimeStamp.Second);

				fileOperations.Add(new FileOperation(fileName, operationType, ioSize, durationInMilliseconds, endTimeStamp, resourceInformation));
			};
		}

		private List<ResourceToMonitor> GetResourcesToMonitor()
		{
			var adminStatistics = store.DatabaseCommands.GlobalAdmin.GetStatistics();
			var loadedDatabases = adminStatistics.LoadedDatabases.Select(x => x.Name ?? Constants.SystemDatabase);

			var results = new List<ResourceToMonitor>();
			foreach (var database in loadedDatabases)
			{
				var isSystemDatabase = string.Equals(database, Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase);
				var commands = isSystemDatabase ? store.DatabaseCommands.ForSystemDatabase() : store.DatabaseCommands.ForDatabase(database);
				var request = commands.CreateRequest("/debug/config", HttpMethods.Get);
				var configuration = (RavenJObject)request.ReadResponseJson();

				var indexStoragePath = configuration.Value<string>("IndexStoragePath");
				var dataDirectory = configuration.Value<string>("DataDirectory");

				results.Add(new ResourceToMonitor
				{
					ResourceName = database,
					ResourceType = ResourceType.Database,
					Paths = new[]
					{
						new PathInformation { Path = indexStoragePath, PathType = PathType.Index },
						new PathInformation { Path = dataDirectory, PathType = PathType.Data }
					}
				});
			}

			return results;
		}

		private static ResourceInformation GetResourceInformation(string fileName, IEnumerable<ResourceToMonitor> resourcesToMonitor)
		{
			foreach (var resourceToMonitor in resourcesToMonitor)
			{
				var path = resourceToMonitor.GetMatchingPath(fileName);
				if (path == null)
					continue;

				return new ResourceInformation
				{
					ResourceType = resourceToMonitor.ResourceType,
					ResourceName = resourceToMonitor.ResourceName,
					PathType = path.PathType
				};
			}

			throw new InvalidOperationException("Could not find matching resource.");
		}

		public void Start()
		{
			StartProcessingOperations();

			session.Source.Process();
		}

		public void Stop()
		{
			DisposeSession();
		}

		private void PersistRun()
		{
			if (store == null)
				return;


			using (var s = store.OpenSession())
			{
				s.Store(run);
				s.SaveChanges();
			}
		}

		private string GenerateDisplayName(DiskIoPerformanceRun diskIoPerformanceRun)
		{
			return string.Format("Monitoring of {0} (PID={1}) for {2} minutes at {3}", 
				diskIoPerformanceRun.ProcessName, 
				diskIoPerformanceRun.ProcessId, 
				diskIoPerformanceRun.DurationInMinutes,
				diskIoPerformanceRun.StartTime.ToLocalTime().ToString("G"));
		}

		private void StartProcessingOperations()
		{
			isRunning = true;
			processingTask = Task.Factory.StartNew(() =>
			{
				while (isRunning)
				{
					var batch = new List<FileOperation>();
					FileOperation fileOperation;
					while (fileOperations.TryTake(out fileOperation, TimeSpan.FromMilliseconds(100)))
						batch.Add(fileOperation);

					ProcessOperations(batch);
				}
			}, TaskCreationOptions.LongRunning);
		}

		private void StopProcessingOperations()
		{
			isRunning = false;

			if (processingTask != null)
				processingTask.Wait();
		}

		public void Dispose()
		{
			DisposeSession();

			StopProcessingOperations();

			PersistRun();

			if (store != null)
				store.Dispose();

			if (timer != null)
				timer.Dispose();
		}

		private void DisposeSession()
		{
			lock (locker)
			{
				if (session == null)
					return;
				session.Dispose();
				session = null;
			}
		}

		private void ProcessOperations(IReadOnlyCollection<FileOperation> ops)
		{
			if (ops.Count == 0)
				return;

			foreach (var operation in ops)
			{
				if (operation.ResourceInformation.ResourceType == ResourceType.FileSystem)
					continue;

				var databaseOperations = run.Databases.FirstOrDefault(x => string.Equals(x.Name, operation.ResourceInformation.ResourceName, StringComparison.OrdinalIgnoreCase));
				if (databaseOperations == null)
				{
					databaseOperations = new DiskIoPerformanceRun.Result(operation.ResourceInformation.ResourceName);
					run.Databases.Add(databaseOperations);
				}

				var results = databaseOperations.Results.GetOrAdd(operation.EndTimeStamp);
				var result = results.FirstOrDefault(x => x.PathType == operation.ResourceInformation.PathType);
				if (result == null)
				{
					result = new DiskIoPerformanceRun.IoResult(operation.ResourceInformation.PathType);
					results.Add(result);
				}

				result.AddOperation(operation);
			}
		}

		private class FileIoData
		{
			public FileIoData(string fileName, OperationType operationType, double timeStampRelativeInMilliseconds, int ioSizeInBytes)
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
	}
}