namespace Raven.Studio.Messages
{
	using System;
	using Caliburn.Micro;

	public class DatabaseScreenRequested
	{
		readonly Func<IScreen> buildViewModel;

		public DatabaseScreenRequested(Func<IScreen> buildViewModel)
		{
			this.buildViewModel = buildViewModel;
		}

		public IScreen GetScreen()
		{
			if (buildViewModel == null)
				throw new ArgumentNullException("You must provide a function to construct an instance of IScreen to display.");
			return buildViewModel();
		}
	}
}