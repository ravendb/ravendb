using JetBrains.Annotations;

namespace Lambda2Js
{
    /// <summary>
    /// Interface that contains metadata information about a property for JavaScript conversion.
    /// </summary>
    public interface IJavascriptMemberMetadata
    {
        /// <summary>
        /// Gets or sets the name of the property when converted to JavaScript.
        /// </summary>
        [NotNull]
        string MemberName { get; }
    }
}