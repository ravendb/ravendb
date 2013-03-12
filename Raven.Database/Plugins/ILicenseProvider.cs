using System.ComponentModel.Composition;

namespace Raven.Database.Plugins
{
	/// <summary>
	/// Provides a RavenDB license programatically.
	/// </summary>
	[InheritedExport]
	public interface ILicenseProvider
	{
		string License { get; }
	}
}
