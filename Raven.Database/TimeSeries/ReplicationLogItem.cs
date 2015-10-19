namespace Raven.Database.TimeSeries
{
	public class ReplicationLogItem
	{
		public long Etag { get; set; }

		public byte[] BinaryData { get; set; }
	}
}