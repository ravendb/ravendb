using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database.FileSystem.Synchronization.Rdc.Wrapper;

namespace Raven.Database.FileSystem.Synchronization.Rdc
{
	internal class NeedListParser
	{
		public static async Task ParseAsync(IPartialDataAccess source, IPartialDataAccess seed, Stream output,
											IEnumerable<RdcNeed> needList, CancellationToken token)
		{
			foreach (var item in needList)
			{
				token.ThrowIfCancellationRequested();

				switch (item.BlockType)
				{
					case RdcNeedType.Source:
						await source.CopyToAsync(output, Convert.ToInt64(item.FileOffset), Convert.ToInt64(item.BlockLength)).ConfigureAwait(false);
						break;
					case RdcNeedType.Seed:
						await seed.CopyToAsync(output, Convert.ToInt64(item.FileOffset), Convert.ToInt64(item.BlockLength)).ConfigureAwait(false);
						break;
					default:
						throw new NotSupportedException();
				}
			}
		}
	}
}