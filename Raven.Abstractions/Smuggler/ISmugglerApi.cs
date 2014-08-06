using System.Threading.Tasks;

namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
        Task<ExportDataResult> ExportData(SmugglerExportOptions exportOptions);

		Task ImportData(SmugglerImportOptions importOptions);
	}
}