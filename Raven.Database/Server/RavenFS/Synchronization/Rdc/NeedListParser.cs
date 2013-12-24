using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Database.Server.RavenFS.Synchronization.Rdc.Wrapper;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc
{
	public class NeedListParser
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
						await source.CopyToAsync(output, Convert.ToInt64(item.FileOffset), Convert.ToInt64(item.BlockLength));
						break;
					case RdcNeedType.Seed:
						await seed.CopyToAsync(output, Convert.ToInt64(item.FileOffset), Convert.ToInt64(item.BlockLength));
						break;
					default:
						throw new NotSupportedException();
				}
			}
		}
	}
}