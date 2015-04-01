using Newtonsoft.Json.Linq;

namespace TailFeather.Storage
{
	public class KeyValueOperation
	{
		public KeyValueOperationTypes Type;
		public string Key;
		public JToken Value;
	}
}