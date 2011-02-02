namespace Raven.Studio.Plugin
{
	using Caliburn.Micro;

	public interface IRavenScreen : IScreen
	{
		SectionType Section { get; }
	}
}