using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using System.IO;
using Raven.Database.Server.RavenFS.Storage;
using Raven.Database.Server.RavenFS.Util;

namespace Raven.Database.Server.RavenFS.Extensions
{
	public static class StreamExtensions
	{
		private static async Task ReadAsync(this Stream self, byte[] buffer, int start, List<int> reads)
		{
			var item = await self.ReadAsync(buffer, start, buffer.Length - start);

			reads.Add(item);
			if (item == 0 || item + start >= buffer.Length)
				return;
			await self.ReadAsync(buffer, start + item, reads);
		}

		private static async Task<int> ReadAsync(this Stream self, byte[] buffer, int start)
		{
			var reads = new List<int>();
			await self.ReadAsync(buffer, start, reads);

			return reads.Sum();
		}

		public static Task<int> ReadAsync(this Stream self, byte[] buffer)
		{
			return self.ReadAsync(buffer, 0);
		}

		public static Task CopyToAsync(this Stream self, Stream destination, long from, long to)
		{
			var limitedStream = new NarrowedStream(self, from, to);
			return limitedStream.CopyToAsync(destination, StorageConstants.MaxPageSize);
		}
	}
}