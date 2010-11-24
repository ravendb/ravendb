namespace Raven.ManagementStudio.UI.Silverlight.Models
{
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using Raven.Database.Indexing;

    public class Index
    {
        public Index(KeyValuePair<string, IndexDefinition> index)
        {
            this.Name = index.Key;
            this.Definition = index.Value;
        }

        public string Name { get; set; }

        [Display(Description = "LINQ query")]
        public string Map
        {
            get { return this.Definition.Map; }
            set { this.Definition.Map = value; }
        }

        public IndexDefinition Definition { get; set; }

        public bool IsEdited { get; set; }
    }
}