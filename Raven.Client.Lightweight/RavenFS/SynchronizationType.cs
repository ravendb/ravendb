namespace Raven.Client.RavenFS
{
	public enum SynchronizationType
	{
		Unknown = 0,
		ContentUpdate = 1,
		MetadataUpdate = 2,
		Rename = 3,
		Delete = 4,
	}
}