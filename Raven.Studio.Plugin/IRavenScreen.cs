namespace Raven.Studio.Plugin
{
	using Caliburn.Micro;

	public interface IRavenScreen : IScreen
	{
		IRavenScreen ParentRavenScreen { get; }

		SectionType Section { get; }
	}
}