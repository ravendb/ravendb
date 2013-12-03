using System.Threading.Tasks;

namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
        Task<ExportDataResult> ExportData(SmugglerExportOptions exportOptions, SmugglerOptions options);

		Task ImportData(SmugglerImportOptions importOptions, SmugglerOptions options);
	}
}