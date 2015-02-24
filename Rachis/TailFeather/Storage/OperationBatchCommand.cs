using Rachis.Commands;

namespace TailFeather.Storage
{
	public class OperationBatchCommand : Command
	{
		public KeyValueOperation[] Batch { get; set; }
	}

    public class GetCommand : Command
    {
        public string Key { get; set; }
    }
}