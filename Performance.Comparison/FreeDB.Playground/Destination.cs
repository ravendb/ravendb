using XmcdParser;

namespace FreeDB.Playground
{
	public abstract class Destination
	{
		public abstract void Accept(Disk d);
		public abstract void Done();
	}
}