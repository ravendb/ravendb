using System.Threading.Tasks;

namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
        Task<ExportDataResult> ExportData(SmugglerExportOptions exportOptions, SmugglerOptionsBase options);

		Task ImportData(SmugglerImportOptions importOptions, SmugglerOptionsBase options);
	}
}