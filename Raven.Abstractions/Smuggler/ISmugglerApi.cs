using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Smuggler.Data;

namespace Raven.Abstractions.Smuggler
{
    public interface ISmugglerApi<TIn, TOptions, TOut>
        where TIn : ConnectionStringOptions, new()
        where TOptions : SmugglerOptions<TIn>
    {
        TOptions Options { get; }

        Task<TOut> ExportData(SmugglerExportOptions<TIn> exportOptions);

        Task ImportData(SmugglerImportOptions<TIn> importOptions);

        Task Between(SmugglerBetweenOptions<TIn> betweenOptions);
	}
}