namespace Raven.Studio.Messages
{
	using Plugin;

	public class OpenNewScreen
	{
		public OpenNewScreen(IRavenScreen screen)
		{
			NewScreen = screen;
		}

		public IRavenScreen NewScreen { get; private set; }
	}
}