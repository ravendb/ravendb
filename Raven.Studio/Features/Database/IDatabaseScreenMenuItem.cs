namespace Raven.Studio.Features.Database
{
    using System.ComponentModel.Composition;
    using Caliburn.Micro;

    [InheritedExport]
	public interface IDatabaseScreenMenuItem : IScreen
	{
		int Index {get;}
	}
}