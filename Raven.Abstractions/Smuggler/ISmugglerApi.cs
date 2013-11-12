using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
        Task<ExportDataResult> ExportData(SmugglerOptions options, PeriodicBackupStatus backupStatus);

		Task ImportData(SmugglerOptions options);
	}
}