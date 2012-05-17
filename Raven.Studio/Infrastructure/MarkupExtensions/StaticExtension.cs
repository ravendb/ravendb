using System;
using System.Reflection;
using System.Windows.Markup;
using System.Xaml;

namespace Raven.Studio.Infrastructure.MarkupExtensions
{
    public class StaticExtension : IMarkupExtension<object>
    {
        public string Member { get; set; }

        public StaticExtension()
        {
            
        }

        public StaticExtension(string member)
        {
            Member = member;
        }

        public object ProvideValue(IServiceProvider serviceProvider)
        {
            if (string.IsNullOrEmpty(Member))
            {
                throw new ArgumentException("No Member was provided");
            }

            if (Member.IndexOf(".") < 0)
            {
                throw new ArgumentException("Member expression does not reference a member.");
            }

            var typeName = Member.Substring(0, Member.IndexOf("."));
            var resolver = serviceProvider.GetService(typeof(IXamlTypeResolver)) as IXamlTypeResolver;

            var type = resolver.Resolve(typeName);

            var memberName = Member.Substring(Member.IndexOf(".") + 1);

            if (type.IsEnum)
            {
                return Enum.Parse(type, memberName, ignoreCase: false);
            }

            var fieldInfo = type.GetField(memberName, BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
            if (fieldInfo != null)
            {
                return fieldInfo.GetValue(null);
            }

            var propertyInfo = type.GetProperty(memberName, BindingFlags.Public | BindingFlags.Static | BindingFlags.FlattenHierarchy);
            if (propertyInfo != null)
            {
                return propertyInfo.GetValue(null, null);
            }

            throw new ArgumentException(string.Format("Type '{0}' has no static member called '{1}'", type.Name, memberName));
        }
    }
}
