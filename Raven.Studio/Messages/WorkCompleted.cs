namespace Raven.Studio.Messages
{
	public class WorkCompleted
	{
		public WorkCompleted() { }
		public WorkCompleted(string job) { Job = job; }

		public string Job { get; private set; }
	}
}