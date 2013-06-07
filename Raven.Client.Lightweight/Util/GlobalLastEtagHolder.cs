using Raven.Abstractions.Data;

namespace Raven.Client.Util
{
	public class GlobalLastEtagHolder : ILastEtagHolder
	{
		private class EtagHolder
		{
			public Etag Etag;
		}

		private volatile EtagHolder lastEtag;
		protected readonly object lastEtagLocker = new object();

        public void UpdateLastWrittenEtag(Etag etag)
		{
			if (etag == null)
				return;

			if (lastEtag == null)
			{
				lock (lastEtagLocker)
				{
					if (lastEtag == null)
					{
						lastEtag = new EtagHolder
						{
							Etag = etag
						};
						return;
					}
				}
			}

			// not the most recent etag
			if (lastEtag.Etag.CompareTo(etag) >= 0)
			{
				return;
			}

			lock (lastEtagLocker)
			{
				// not the most recent etag
                if (lastEtag.Etag.CompareTo(etag) >= 0)
				{
					return;
				}

				lastEtag = new EtagHolder
				{
					Etag = etag,
				};
			}
		}

		
		public Etag GetLastWrittenEtag()
		{
			var etagHolder = lastEtag;
			if (etagHolder == null)
				return null;
			return etagHolder.Etag;
		}
	}
}