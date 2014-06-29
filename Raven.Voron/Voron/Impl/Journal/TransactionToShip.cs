using System.IO;

namespace Voron.Impl.Journal
{
	public class TransactionToShip
	{
		public TransactionHeader Header { get; private set; }

		public uint PreviousTransactionCrc { get; set; }

		public Stream CompressedData { get; set; }

		public TransactionToShip(TransactionHeader header)
		{
			Header = header;
		}
	}
}