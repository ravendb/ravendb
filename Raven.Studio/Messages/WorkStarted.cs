namespace Raven.Studio.Messages
{
	public class WorkStarted
	{
		public WorkStarted() { }
		public WorkStarted(string job) { Job = job; }

		public string Job { get; private set; }
	}
}