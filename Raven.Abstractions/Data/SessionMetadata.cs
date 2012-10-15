namespace Raven.Abstractions.Data
{
	public class SessionMetadata
	{
		public long SaveCounter { get; set; }

		public SessionMetadata Clone()
		{
			return (SessionMetadata)MemberwiseClone();
		}
	}
}