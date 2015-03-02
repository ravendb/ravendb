using System.Threading.Tasks;

using Raven.Imports.Newtonsoft.Json;

namespace Rachis.Commands
{
	public abstract class Command
	{
		public long AssignedIndex { get; set; }

		[JsonIgnore]
		public TaskCompletionSource<object> Completion { get; set; }

        [JsonIgnore]
        public object CommandResult { get; set; }

		public bool BufferCommand { get; set; }

	    public void Complete()
	    {
	        if (Completion == null)
	            return;
            Completion.SetResult(CommandResult);
	    }
	}
}
