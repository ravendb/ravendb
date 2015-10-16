using System;
using System.Threading.Tasks;
using Raven.Abstractions.Data;

namespace Raven.Smuggler.Database
{
	public interface IDatabaseSmugglerDocumentActions : IDisposable
	{
		Task WriteDocumentAsync(JsonDocument document);
	}
}