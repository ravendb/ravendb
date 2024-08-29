namespace Raven.Client.Documents.Indexes
{
    /// <summary>
    /// Defines the priority levels that control the order of index processing.
    /// </summary>
    /// <value>
    /// <list type="bullet">
    /// <item>
    /// <term>Low</term>
    /// <description>Assigns a lower processing priority to the index.</description>
    /// </item>
    /// <item>
    /// <term>Normal</term>
    /// <description>Assigns a normal processing priority to the index.</description>
    /// </item>
    /// <item>
    /// <term>High</term>
    /// <description>Assigns a higher processing priority to the index.</description>
    /// </item>
    /// </list>
    /// </value>
    public enum IndexPriority
    {
        Low,
        Normal,
        High
    }
}