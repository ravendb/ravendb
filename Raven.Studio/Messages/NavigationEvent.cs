namespace Raven.Studio.Messages
{
	public class NavigationEvent
	{
		readonly string name;
		readonly System.Action undo;

		public NavigationEvent(string name, System.Action undo)
		{
			this.name = name;
			this.undo = undo;
		}

		public void Reverse()
		{
			undo();
		}
	}
}