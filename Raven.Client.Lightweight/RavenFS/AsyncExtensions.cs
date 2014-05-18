using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.RavenFS
{
	public static class AsyncExtensions
	{
		public static Task CopyToAsync(this Stream source, Stream destination, Action<long> progressReport, int bufferSize = 0x1000)
		{
			return CopyStream(source, destination, progressReport, bufferSize, CancellationToken.None);
		}

		public static Task CopyToAsync(this Stream source, Stream destination, Action<long> progressReport, CancellationToken token, int bufferSize = 0x1000)
		{
			return CopyStream(source, destination, progressReport, bufferSize, token);
		}

		private static Task CopyStream(Stream source, Stream destination, Action<long> progressReport,
											   int bufferSize, CancellationToken token)
		{
			var listenableStream = new ListenableStream(source);
			listenableStream.ReadingProgress += (_, progress) => progressReport(progress.Processed);
			return listenableStream.CopyToAsync(destination, bufferSize, token);
		}
	}
}
