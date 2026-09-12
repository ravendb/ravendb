namespace Raven.Client.Documents.Conventions
{
    /// <summary>
    /// Controls the patch command format used by the session <c>Patch</c> methods
    /// (property set, array Add/RemoveAt, dictionary Add/Remove).
    /// </summary>
    public enum SessionPatchBehavior
    {
        /// <summary>
        /// Generate RFC 6902 JsonPatch commands where possible (default). Operations without a
        /// JsonPatch equivalent still fall back to JavaScript-based patch commands.
        /// </summary>
        JsonPatch,

        /// <summary>
        /// Always generate JavaScript-based patch commands (the behavior prior to JsonPatch support).
        /// Use this to opt out of the JsonPatch code path entirely.
        /// </summary>
        JavaScript
    }
}
