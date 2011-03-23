namespace Raven.Studio.Features.Collections
{
	using Framework;

	public class BuiltinCollectionName : Enumeration<string>
	{
		public static BuiltinCollectionName Document = new BuiltinCollectionName("document");
		public static BuiltinCollectionName Projection = new BuiltinCollectionName("projection");
		public static BuiltinCollectionName System = new BuiltinCollectionName("System");

		public BuiltinCollectionName(string value) : base(value, value)
		{
		}
	}
}