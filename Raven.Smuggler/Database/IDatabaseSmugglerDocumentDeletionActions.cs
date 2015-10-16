using System;
using System.Threading.Tasks;

namespace Raven.Smuggler.Database
{
	public interface IDatabaseSmugglerDocumentDeletionActions : IDisposable
	{
		Task WriteDocumentDeletionAsync(string key);
	}
}