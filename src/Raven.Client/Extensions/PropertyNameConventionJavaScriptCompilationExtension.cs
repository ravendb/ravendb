using System.Reflection;
using Lambda2Js;
using Raven.Client.Documents.Conventions;

namespace Raven.Client.Extensions
{
    internal class PropertyNameConventionJSMetadataProvider : JavascriptMetadataProvider
    {
        private readonly DocumentConventions _conventions;

        public PropertyNameConventionJSMetadataProvider(DocumentConventions conventions)
        {
            _conventions = conventions;
        }

        public class MemberMetadata : IJavascriptMemberMetadata
        {
            public string MemberName { get; set; }
        }

        public override IJavascriptMemberMetadata GetMemberMetadata(MemberInfo memberInfo)
        {
            return new MemberMetadata
            {
                MemberName = _conventions.GetConvertedPropertyNameFor(memberInfo)
            };
        }
    }
}
