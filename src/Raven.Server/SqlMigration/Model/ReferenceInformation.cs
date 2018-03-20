using System;
using System.Collections.Generic;
using Raven.Server.SqlMigration.Schema;
using Sparrow.Json.Parsing;

namespace Raven.Server.SqlMigration.Model
{
    public class ReferenceInformation
    {
        public string PropertyName { get; set; }
        
        public string SourceTableName { get; set; }
        
        public List<string> SourcePrimaryKeyColumns { get; set; }
        public List<string> TargetPrimaryKeyColumns { get; set; }
        public List<string> ForeignKeyColumns { get; set; }
        
        public string CollectionNameToUseInLinks { get; set; }
        public IDataProvider<object> DataProvider {get; set; }
        public HashSet<string> TargetSpecialColumnsNames { get; set; }
        public HashSet<string> TargetDocumentColumns { get; set; }
        public HashSet<string> TargetAttachmentColumns { get; set; }
        
        public ReferenceType Type { get; set; }
        
        public List<ReferenceInformation> ChildReferences { get; set;}
    }
    
    public enum ReferenceType {
        ArrayLink,
        ArrayEmbed,
        ObjectLink,
        ObjectEmbed
    }
}
