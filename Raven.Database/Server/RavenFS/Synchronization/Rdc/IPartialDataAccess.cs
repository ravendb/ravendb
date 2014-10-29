using System.IO;
using System.Threading.Tasks;

namespace Raven.Database.Server.RavenFS.Synchronization.Rdc
{
	public interface IPartialDataAccess
	{
		Task CopyToAsync(Stream target, long from, long length);
	}
}