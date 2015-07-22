namespace Raven.Monitor.IO.Data
{
	public class FileOperation
	{
		public string FileName { get; private set; }

		public OperationType OperationType { get; set; }

		public int IoSizeInBytes { get; private set; }

		public double DurationInMilliseconds { get; private set; }

		public FileOperation(string fileName, OperationType operationType, int ioSizeInBytes, double durationInMilliseconds)
		{
			FileName = fileName;
			OperationType = operationType;
			IoSizeInBytes = ioSizeInBytes;
			DurationInMilliseconds = durationInMilliseconds;
		}
	}
}