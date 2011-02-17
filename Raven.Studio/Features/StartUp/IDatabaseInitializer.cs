namespace Raven.Studio.Features.StartUp
{
	using System.Collections.Generic;
	using System.Threading.Tasks;
	using Client;

	public interface IDatabaseInitializer
	{
		IEnumerable<Task> Initialize(IAsyncDocumentSession session);
	}
}