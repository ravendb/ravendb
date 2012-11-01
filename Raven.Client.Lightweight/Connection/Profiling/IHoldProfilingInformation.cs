namespace Raven.Client.Connection.Profiling
{
	/// <summary>
	/// Interface for getting profiling information about the actions of the system
	/// within a given context, usually the context is database commands or async database commands
	/// </summary>
	public interface IHoldProfilingInformation
	{
		/// <summary>
		/// The profiling information
		/// </summary>
		ProfilingInformation ProfilingInformation { get; }
	}
}