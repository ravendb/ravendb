using System.Globalization;
using Raven.Abstractions.Data;
using Raven.Studio.Infrastructure;
using Raven.Studio.Models;

namespace Raven.Studio.Commands
{
	public class SaveBundlesCommand : Command
	{
		private readonly BundlesModel bundlesModel;

		public SaveBundlesCommand(BundlesModel bundlesModel)
		{
			this.bundlesModel = bundlesModel;
		}

		public override void Execute(object parameter)
		{
			if (bundlesModel.HasQuotas)
			{
				bundlesModel.DatabaseDocument.Settings[Constants.SizeHardLimitInKB] =
					(bundlesModel.MaxSize*1024).ToString(CultureInfo.InvariantCulture);
				bundlesModel.DatabaseDocument.Settings[Constants.SizeSoftLimitInKB] =
					(bundlesModel.WarnSize*1024).ToString(CultureInfo.InvariantCulture);
				bundlesModel.DatabaseDocument.Settings[Constants.DocsHardLimit] =
					(bundlesModel.MaxDocs).ToString(CultureInfo.InvariantCulture);
				bundlesModel.DatabaseDocument.Settings[Constants.DocsSoftLimit] =
					(bundlesModel.WarnDocs).ToString(CultureInfo.InvariantCulture);
			}

			if (bundlesModel.HasReplication)
			{
				//TODO: save replication
			}

			if (bundlesModel.HasVersioning)
			{
				//TODO: save versioning
			}
		}
	}
}
