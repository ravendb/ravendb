namespace Rachis.Messages
{
	public class LogEntry
	{
		public long Index { get; set; }
		public long Term { get; set; }
		public bool? IsTopologyChange { get; set; }
        public byte[] Data { get; set; }
	}
}