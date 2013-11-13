using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
        Task<ExportDataResult> ExportData(SmugglerExportOptions options, PeriodicBackupStatus backupStatus);

		Task ImportData(SmugglerImportOptions options);
	}
}