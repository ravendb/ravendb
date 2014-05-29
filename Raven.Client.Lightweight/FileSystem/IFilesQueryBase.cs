using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public interface IFilesQueryBase<T, out TSelf> 
        where TSelf : IFilesQueryBase<T, TSelf>
        where T : IRemoteObject
    {
        /// <summary>
        /// Gets the files convention from the query session
        /// </summary>
        FilesConvention Convention { get; }


        /// <summary>
        /// Disables tracking for queried entities by Raven's Unit of Work.
        /// Usage of this option will prevent modifying and interacting with the IRemoteObject returned.
        /// </summary>
        TSelf NoTracking();

        /// <summary>
        /// Disables caching for query results.
        /// </summary>
        TSelf NoCaching();


        /// <summary>
        /// Provide statistics about the query, such as total count of matching records
        /// </summary>
        TSelf Statistics(out FilesQueryStatistics stats);



        /// <summary>
        ///   Takes the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        TSelf Take(int count);

        /// <summary>
        ///   Skips the specified count.
        /// </summary>
        /// <param name = "count">The count.</param>
        /// <returns></returns>
        TSelf Skip(int count);

        /// <summary>
        ///   Returns first element or default value for type if sequence is empty.
        /// </summary>
        /// <returns></returns>
        T FirstOrDefault();

        /// <summary>
        ///   Returns first element or throws if sequence is empty.
        /// </summary>
        /// <returns></returns>
        T First();

        /// <summary>
        ///   Returns first element or default value for given type if sequence is empty. Throws if sequence contains more than one element.
        /// </summary>
        /// <returns></returns>
        T SingleOrDefault();

        /// <summary>
        ///   Returns first element or throws if sequence is empty or contains more than one element.
        /// </summary>
        /// <returns></returns>
        T Single();
    }
}
