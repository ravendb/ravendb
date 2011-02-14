namespace Raven.Studio.Messages
{
	public class NavigationOccurred
	{
		readonly string name;
		readonly System.Action undo;

		public NavigationOccurred(string name, System.Action undo)
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