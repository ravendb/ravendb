namespace Raven.Management.Client.Silverlight.Document
{
    using System.Collections.Generic;
    using Database.Data;

    /// <summary>
    /// Data for a batch command to the server
    /// </summary>
    public class SaveChangesData
    {
        /// <summary>
        /// Gets or sets the commands.
        /// </summary>
        /// <value>The commands.</value>
        public IList<ICommandData> Commands { get; set; }

        /// <summary>
        /// Gets or sets the entities.
        /// </summary>
        /// <value>The entities.</value>
        public IList<object> Entities { get; set; }
    }
}