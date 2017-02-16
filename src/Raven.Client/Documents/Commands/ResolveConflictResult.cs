using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Commands
{
    public class ResolveConflictResult
    {
        public enum ResultType : byte
        {
            Resolved,
            ChangeVectorNotFound,
            NotConflicted
        }

        public ResultType Result { get; set; }

        public long ResolvedEtag { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(ResolvedEtag)] = ResolvedEtag,
                [nameof(Result)] = Result
            };
        }
    }
}
