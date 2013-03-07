using System.IO;
using System.Threading.Tasks;

namespace Raven.Abstractions.Smuggler
{
	public interface ISmugglerApi
	{
		Task<string> ExportData(SmugglerOptions options, bool incremental);
		Task ImportData(Stream stream, SmugglerOptions options, bool incremental);
	}
}