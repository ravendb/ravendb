using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System
{
    public class ResourceDeleteResult
    {
        public string QualifiedName { get; set; }

        public bool Deleted { get; set; }

        public string Reason { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(QualifiedName)] = QualifiedName,
                [nameof(Deleted)] = Deleted,
                [nameof(Reason)] = Reason
            };
        }
    }
}