using System.IO;
using System.Threading.Tasks;

namespace Raven.Database.FileSystem.Synchronization.Rdc
{
	public interface IPartialDataAccess
	{
		Task CopyToAsync(Stream target, long from, long length);
	}
}