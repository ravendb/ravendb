namespace Raven.Server.Utils
{
    /// <summary>
    /// A reference that can be used with lambda expression
    /// to pass a value out.
    /// </summary>
    public sealed class Reference<T>
    {
        /// <summary>
        /// Gets or sets the value.
        /// </summary>
        /// <value>The value.</value>
        public T Value { get; set; }
    }
}