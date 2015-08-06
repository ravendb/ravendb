// -----------------------------------------------------------------------
//  <copyright file="DiskIoPerformanceRun.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;

namespace Raven.Monitor.IO.Data
{
	internal class DiskIoPerformanceRun
	{
		public DiskIoPerformanceRun()
		{
			Databases = new List<Result>();
		}

		public string DisplayName { get; set; }

		public int ProcessId { get; set; }

		public string ProcessName { get; set; }

		public int DurationInMinutes { get; set; }

		public DateTime StartTime { get; set; }

		public List<Result> Databases { get; set; }

		internal class Result
		{
			public string Name { get; private set; }

			public Dictionary<DateTime, List<IoResult>> Results { get; private set; }

			public Result(string resourceName)
			{
				Name = resourceName;
				Results = new Dictionary<DateTime, List<IoResult>>();
			}
		}

		internal class IoResult
		{
			public IoResult(PathType pathType)
			{
				PathType = pathType;
			}

			public PathType PathType { get; private set; }

			public double WriteDurationInMilliseconds { get; private set; }

			public double WriteIoSizeInBytes { get; private set; }

			public double ReadDurationInMilliseconds { get; private set; }

			public double ReadIoSizeInBytes { get; private set; }

			public int NumberOfReadOperations { get; private set; }

			public int NumberOfWriteOperations { get; private set; }

			public void AddOperation(FileOperation operation)
			{
				Debug.Assert(operation.ResourceInformation.PathType == PathType);

				switch (operation.OperationType)
				{
					case OperationType.Read:
						ReadDurationInMilliseconds += operation.DurationInMilliseconds;
						ReadIoSizeInBytes += operation.IoSizeInBytes;
						NumberOfReadOperations++;
						break;
					case OperationType.Write:
						WriteDurationInMilliseconds += operation.DurationInMilliseconds;
						WriteIoSizeInBytes += operation.IoSizeInBytes;
						NumberOfWriteOperations++;
						break;
				}
			}
		}
	}
}