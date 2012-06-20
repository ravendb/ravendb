namespace Raven.Abstractions.Logging
{
	public interface ILogProvider
	{
		ILog GetLogger(string name);
	}
}