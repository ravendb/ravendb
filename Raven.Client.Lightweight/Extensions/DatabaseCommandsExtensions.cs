namespace Raven.Client.Extensions
{
#if !NET35
	using Raven.Client.Connection.Async;
	using System.Threading.Tasks;
	using Indexes;

	///<summary>
	/// Extension methods that make certain database command operations more convenient to use
	///</summary>
	public static class DatabaseCommandsExtensions
	{
		/// <summary>
		/// Asynchronously creates an index
		/// </summary>
		/// <typeparam name="T">The type that defines the index to be create.</typeparam>
		/// <param name="commands">The hook to the database commands.</param>
		/// <param name="overwrite">Should the index be overwritten if it already exists.</param>
		/// <returns></returns>
		public static Task<string> PutIndexAsync<T>(this IAsyncDatabaseCommands commands, bool overwrite)
			where T : AbstractIndexCreationTask, new()
		{
			var indexCreationTask = new T();

			return commands.PutIndexAsync(
				indexCreationTask.IndexName,
				indexCreationTask.CreateIndexDefinition(), overwrite);
		}
	}
#endif
}