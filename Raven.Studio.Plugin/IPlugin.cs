namespace Raven.Studio.Plugin
{
	public interface IPlugin : IMenuItem
	{
		string Name { get; }

		IServer Server { get; set; }

		void GoToScreen();
	}
}