using System.IO;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
		Task<string> ExportData(Stream stream, SmugglerOptions options, bool incremental, PeriodicBackupStatus backupStatus);
		Task ImportData(Stream stream, SmugglerOptions options);
	}
}