using System;

namespace Raven.Client.Util
{
	public interface ILastEtagHolder
	{
		void UpdateLastWrittenEtag(Guid? etag);
		Guid? GetLastWrittenEtag();
	}
}