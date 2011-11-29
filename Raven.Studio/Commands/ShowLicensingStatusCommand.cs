// -----------------------------------------------------------------------
//  <copyright file="ShowLicensingStatusCommand.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;

namespace Raven.Studio.Commands
{
	public class ShowLicensingStatusCommand : Command
	{
		public override void Execute(object parameter)
		{
			var licensing = (LicensingStatus) parameter;
			ErrorPresenter.Show("Licensing Status", licensing.Message);
		}
	}
}