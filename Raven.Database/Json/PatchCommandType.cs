namespace Raven.Database.Json
{
    /// <summary>
    /// Patch command options
    /// </summary>
    public enum PatchCommandType
    {
        /// <summary>
        /// Set a property
        /// </summary>
        Set,
        /// <summary>
        /// Unset (remove) a property
        /// </summary>
        Unset,
        /// <summary>
        /// Add an item to an array
        /// </summary>
        Add,
        /// <summary>
        /// Insert an item to an array at a specified position
        /// </summary>
        Insert,
        /// <summary>
        /// Remove an item from an array at a specified position
        /// </summary>
        Remove,
        /// <summary>
        /// Modify a property value by providing a nested set of patch operation
        /// </summary>
        Modify,
        /// <summary>
        /// Increment a property by a specified value
        /// </summary>
        Inc,
        /// <summary>
        /// Copy a property value to another property
        /// </summary>
        Copy,
        /// <summary>
        /// Rename a property
        /// </summary>
        Rename
    }
}