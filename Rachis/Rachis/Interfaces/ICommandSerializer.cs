using Rachis.Commands;

namespace Rachis.Interfaces
{
	public interface ICommandSerializer
	{
		byte[] Serialize(Command cmd);
		Command Deserialize(byte[] cmd);
	}
}
