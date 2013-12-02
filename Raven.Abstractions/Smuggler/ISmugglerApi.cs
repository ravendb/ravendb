using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
        Task<ExportDataResult> ExportData(SmugglerExportOptions exportOptions, SmugglerOptionsBase options, PeriodicBackupStatus backupStatus);

		Task ImportData(SmugglerImportOptions importOptions, SmugglerOptionsBase options);
	}
}