using System.Threading.Tasks;

using Raven.Abstractions.Smuggler.Data;

namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
        Task<ExportDataResult> ExportData(SmugglerExportOptions exportOptions);

		Task ImportData(SmugglerImportOptions importOptions);

		Task Between(SmugglerBetweenOptions betweenOptions);
	}
}