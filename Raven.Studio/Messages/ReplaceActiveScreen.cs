namespace Raven.Studio.Messages
{
	using Plugin;

	public class ReplaceActiveScreen
	{
		public ReplaceActiveScreen(IRavenScreen screen)
		{
			NewScreen = screen;
		}

		public IRavenScreen NewScreen { get; private set; }
	}
}