using System;
using System.Threading.Tasks;

using Raven.Json.Linq;

namespace Raven.Smuggler.Database
{
	public interface IDatabaseSmugglerDocumentActions : IDisposable
	{
		Task WriteDocumentAsync(RavenJObject document);
	}
}