using XmcdParser;

namespace FreeDB.Playground
{
	public abstract class DisksDestination
	{
		public abstract void Accept(Disk d);
		public abstract void Done();
	}
}