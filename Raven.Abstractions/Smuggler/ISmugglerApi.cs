using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
		Task<string> ExportData(SmugglerOptions options, PeriodicBackupStatus backupStatus);
		Task ImportData(SmugglerOptions options);
	}
}