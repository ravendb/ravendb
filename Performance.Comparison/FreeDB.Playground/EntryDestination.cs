namespace FreeDB.Playground
{
	public abstract class EntryDestination
	{
		public abstract int Accept(string d);
		public abstract void Done();
	}
}