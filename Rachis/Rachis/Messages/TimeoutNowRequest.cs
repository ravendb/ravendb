namespace Rachis.Messages
{
	public class TimeoutNowRequest : BaseMessage
	{
		public long Term { get; set; }
	}
}