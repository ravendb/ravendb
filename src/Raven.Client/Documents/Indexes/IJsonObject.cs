using System.Collections.Generic;

namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Represents a JSON object that can be used in indexing operations.
    /// Provides access to properties and values in a JSON document structure.
    /// </summary>
    public interface IJsonObject : IEnumerable<KeyValuePair<string, object>>
    {
        /// <summary>
        /// Gets the value of the specified property.
        /// </summary>
        /// <param name="propertyName">The name of the property to retrieve.</param>
        /// <returns>The value of the property, or null if the property does not exist.</returns>
        object this[string propertyName] { get; }

        /// <summary>
        /// Gets the value of the specified property cast to the specified type.
        /// </summary>
        /// <typeparam name="T">The type to cast the value to.</typeparam>
        /// <param name="propertyName">The name of the property to retrieve.</param>
        /// <returns>The value of the property cast to the specified type.</returns>
        T Value<T>(string propertyName);

        /// <summary>
        /// Gets all values in the JSON object cast to the specified type.
        /// </summary>
        /// <typeparam name="T">The type to cast the values to.</typeparam>
        /// <returns>An enumerable of all values cast to the specified type.</returns>
        IEnumerable<T> Values<T>();

        /// <summary>
        /// Represents metadata associated with a JSON object.
        /// </summary>
        public interface IMetadata
        {
            /// <summary>
            /// Gets the value of the specified metadata property.
            /// </summary>
            /// <param name="propertyName">The name of the metadata property to retrieve.</param>
            /// <returns>The value of the metadata property, or null if the property does not exist.</returns>
            object this[string propertyName] { get; }

            /// <summary>
            /// Gets the value of the specified metadata property cast to the specified type.
            /// </summary>
            /// <typeparam name="T">The type to cast the value to.</typeparam>
            /// <param name="propertyName">The name of the metadata property to retrieve.</param>
            /// <returns>The value of the metadata property cast to the specified type.</returns>
            T Value<T>(string propertyName);

            /// <summary>
            /// Gets all metadata values cast to the specified type.
            /// </summary>
            /// <typeparam name="T">The type to cast the values to.</typeparam>
            /// <returns>An enumerable of all metadata values cast to the specified type.</returns>
            IEnumerable<T> Values<T>();
        }
    }
}
