using System;
using Raven.Abstractions.Extensions;

namespace Raven.Client.Util
{
	public class GlobalLastEtagHolder : ILastEtagHolder
	{
		private class EtagHolder
		{
			public Guid Etag;
			public byte[] Bytes;
		}

		private volatile EtagHolder lastEtag;
		protected readonly object lastEtagLocker = new object();

		public void UpdateLastWrittenEtag(Guid? etag)
		{
			if (etag == null)
				return;

			var newEtag = etag.Value.ToByteArray();

			if (lastEtag == null)
			{
				lock (lastEtagLocker)
				{
					if (lastEtag == null)
					{
						lastEtag = new EtagHolder
						{
							Bytes = newEtag,
							Etag = etag.Value
						};
						return;
					}
				}
			}

			// not the most recent etag
			if (Buffers.Compare(lastEtag.Bytes, newEtag) >= 0)
			{
				return;
			}

			lock (lastEtagLocker)
			{
				// not the most recent etag
				if (Buffers.Compare(lastEtag.Bytes, newEtag) >= 0)
				{
					return;
				}

				lastEtag = new EtagHolder
				{
					Etag = etag.Value,
					Bytes = newEtag
				};
			}
		}

		
		public Guid? GetLastWrittenEtag()
		{
			var etagHolder = lastEtag;
			if (etagHolder == null)
				return null;
			return etagHolder.Etag;
		}
	}
}