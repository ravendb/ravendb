using Raven.Abstractions.Data;

namespace Raven.Client.Util
{
	public interface ILastEtagHolder
	{
		void UpdateLastWrittenEtag(Etag etag);
		Etag GetLastWrittenEtag();
	}
}