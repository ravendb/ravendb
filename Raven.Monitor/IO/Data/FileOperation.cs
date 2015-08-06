using System;

namespace Raven.Monitor.IO.Data
{
	internal class FileOperation
	{
		public string FileName { get; private set; }

		public OperationType OperationType { get; set; }

		public int IoSizeInBytes { get; private set; }

		public double DurationInMilliseconds { get; private set; }

		public DateTime EndTimeStamp { get; private set; }

		public ResourceInformation ResourceInformation { get; private set; }

		public FileOperation(string fileName, OperationType operationType, int ioSizeInBytes, double durationInMilliseconds, DateTime endTimeStamp, ResourceInformation resourceInformation)
		{
			FileName = fileName;
			OperationType = operationType;
			IoSizeInBytes = ioSizeInBytes;
			DurationInMilliseconds = durationInMilliseconds;
			EndTimeStamp = endTimeStamp;
			ResourceInformation = resourceInformation;
		}
	}
}