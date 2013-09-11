#if !SILVERLIGHT && !NETFX_CORE
namespace Raven.Client.Document.DTC
{
	public class IsolatedStorageTransactionRecoveryStorage : ITransactionRecoveryStorage
	{
		public ITransactionRecoveryStorageContext Create()
		{
			return new IsolatedStorageTransactionRecoveryContext();
		}
	}
}
#endif