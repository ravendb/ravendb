namespace Raven.Studio.Messages
{
	using Plugin;

	public class ChangeActiveScreen
	{
		public ChangeActiveScreen(IRavenScreen screen)
		{
			ActiveScreen = screen;
		}

		public IRavenScreen ActiveScreen { get; private set; }
	}
}