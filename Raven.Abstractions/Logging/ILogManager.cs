namespace Raven.Abstractions.Logging
{
	public interface ILogManager
	{
		ILog GetLogger(string name);
	}
}