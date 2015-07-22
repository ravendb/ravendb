// -----------------------------------------------------------------------
//  <copyright file="DiskIoPerformanceMonitor.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.IO;
using System.Threading;

using Microsoft.Diagnostics.Tracing.Parsers;
using Microsoft.Diagnostics.Tracing.Session;

using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;
using Raven.Monitor.IO.Data;

namespace Raven.Monitor.IO
{
	internal class DiskIoPerformanceMonitor : IDisposable
	{
		private readonly ConcurrentDictionary<ulong, FileIoData> operations = new ConcurrentDictionary<ulong, FileIoData>();

		private readonly Timer timer;

		private readonly FileStream file;

		private readonly TextWriter writer;

		private readonly object locker = new object();

		private TraceEventSession session;

		public DiskIoPerformanceMonitor(MonitorOptions options)
		{
			file = new FileStream(options.OutputPath, FileMode.CreateNew, FileAccess.Write);
			writer = new StreamWriter(file);

			timer = new Timer(_ => Stop(), null, TimeSpan.FromMinutes(options.IoOptions.DurationInMinutes), TimeSpan.FromMilliseconds(-1));

			session = new TraceEventSession("RavenIO");
			session.EnableKernelProvider(KernelTraceEventParser.Keywords.FileIOInit | KernelTraceEventParser.Keywords.FileIO | KernelTraceEventParser.Keywords.All);

			session.Source.Kernel.FileIORead += data =>
			{
				if (data.ProcessID != options.ProcessId)
					return;

				var fileName = data.FileName;
				if (string.IsNullOrEmpty(fileName))
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

				operations.AddOrUpdate(data.IrpPtr, new FileIoData(fileName, OperationType.Write, data.TimeStampRelativeMSec, data.IoSize), (_, d) =>
				{
					d.IoSizeInBytes += data.IoSize;
					return d;
				});
			};

			session.Source.Kernel.FileIOOperationEnd += data =>
			{
				if (data.ProcessID != options.ProcessId)
					return;

				FileIoData value;
				if (operations.TryRemove(data.IrpPtr, out value) == false)
					return;

				var fileName = value.FileName;
				var ioSize = value.IoSizeInBytes;
				var durationInMilliseconds = data.TimeStampRelativeMSec - value.TimeStampRelativeInMilliseconds;
				var operationType = value.OperationType;

				var fileOperation = new FileOperation(fileName, operationType, ioSize, durationInMilliseconds);
				var json = RavenJObject.FromObject(fileOperation);

				lock (locker)
				{
					writer.WriteLine(json.ToString(Formatting.None));
					writer.Flush();
				}
			};
		}

		public void Start()
		{
			session.Source.Process();
		}

		public void Stop()
		{
			DisposeSession();
		}

		public void Dispose()
		{
			DisposeSession();

			if (timer != null)
				timer.Dispose();

			if (writer != null)
				writer.Flush();

			if (file != null)
				file.Dispose();
		}

		private void DisposeSession()
		{
			if (session == null)
				return;

			lock (locker)
			{
				session.Dispose();
				session = null;
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