namespace Raven.Bundles.Authorization.Model
{
	public interface IPermission
	{
		string Operation { get; set; }
		bool Allow { get; set; }
		int Priority { get; set; }

		string Explain { get; }
	}
}
