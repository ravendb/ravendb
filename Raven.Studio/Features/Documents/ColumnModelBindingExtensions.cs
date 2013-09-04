using System;
using System.Linq;
using System.Text;
using System.Windows;
using System.Windows.Data;
using Raven.Abstractions.Data;
using Raven.Imports.Newtonsoft.Json;
using Raven.Imports.Newtonsoft.Json.Linq;
using Raven.Json.Linq;
using Raven.Studio.Infrastructure.Converters;

namespace Raven.Studio.Features.Documents
{
    public static class ColumnModelBindingExtensions
    {
        private static string ExpandBinding(this string binding)
        {
            if (IsXamlBinding(binding))
                return binding.Substring("$JsonDocument:".Length);
            
			if (IsMetaBinding(binding))
                return "Metadata" + ExpandPropertyPathToXamlBinding(binding.Substring("$Meta:".Length));
            
			if (IsTempScoreBinding(binding))
                return "TempIndexScore";
	        
			return "DataAsJson" + ExpandPropertyPathToXamlBinding(binding);
        }

        private static bool IsTempScoreBinding(string binding)
        {
            return binding == "$Temp:Score";
        }

        private static bool IsMetaBinding(string binding)
        {
            return binding.StartsWith("$Meta:");
        }

        private static bool IsXamlBinding(string binding)
        {
            return binding.StartsWith("$JsonDocument:");
        }

        private static bool IsKeyBinding(string binding)
        {
            return binding.Equals("$JsonDocument:Key", StringComparison.Ordinal);
        }

        private static bool IsEtagBinding(string binding)
        {
            return binding.Equals("$JsonDocument:Etag", StringComparison.Ordinal);
        }

        private static bool IsLastModifiedBinding(string binding)
        {
            return binding.Equals("$JsonDocument:LastModified", StringComparison.Ordinal);
        }


        private static string ExpandPropertyPathToXamlBinding(string binding)
        {
            // for example TestNested[0].MyProperty will be expanded to [TestNested][0][MyProperty]
            var result = binding.Split(new[] { '.' }, StringSplitOptions.RemoveEmptyEntries)
                .SelectMany(s => s.Split(new[] { '[', ']' }, StringSplitOptions.RemoveEmptyEntries))
                .Aggregate(new StringBuilder(), (sb, next) =>
                {
                    sb.Append('[');
                    sb.Append(next);
                    sb.Append(']');
                    return sb;
                }, sb => sb.ToString());

            return result;
        }

        public static string GetBindingPath(this ColumnDefinition columnDefinition, string pathPrefix)
        {
            return pathPrefix + ExpandBinding(columnDefinition.Binding);
        }

        public static Binding CreateBinding(this ColumnDefinition columnDefinition, string pathPrefix)
        {
            return new Binding(GetBindingPath(columnDefinition, pathPrefix)) { Converter = DocumentPropertyToSingleLineStringConverter.Default };
        }

        public static Func<JsonDocument, object> GetValueExtractor(this ColumnDefinition columnDefinition)
        {
            if (IsKeyBinding(columnDefinition.Binding))
                return doc => doc.Key;

            if (IsEtagBinding(columnDefinition.Binding))
                return doc => doc.Etag;

            if (IsLastModifiedBinding(columnDefinition.Binding))
                return doc => doc.LastModified;

            if (IsXamlBinding(columnDefinition.Binding))
                return new BoundValueExtractor(CreateBinding(columnDefinition, "")).ExtractValue;

            if (IsMetaBinding(columnDefinition.Binding))
            {
                return CreateJPathMetaExtractor(columnDefinition.Binding.Substring("$Meta:".Length));
            }

            if (IsTempScoreBinding(columnDefinition.Binding))
                return doc => doc.TempIndexScore;

            return CreateJPathDocumentExtractor(columnDefinition.Binding);
        }

        private static Func<JsonDocument, object> CreateJPathMetaExtractor(string binding)
        {
            try
            {
                var jpath = new RavenJPath(binding);

                return doc => GetTokenValue(doc.Metadata.SelectToken(jpath));
            }
            catch (Exception)
            {
                return doc => "<<Invalid Binding Expression>>";
            }
        }

        private static object GetTokenValue(RavenJToken token)
        {
            if (token == null)
            {
                return "";
            }

            switch (token.Type)
            {
                case JTokenType.Object:
                case JTokenType.Array:
                case JTokenType.Constructor:
                case JTokenType.Property:
                case JTokenType.Comment:
                case JTokenType.Raw:
                case JTokenType.Bytes:
                case JTokenType.Uri:
                    return token.ToString(Formatting.None);
                case JTokenType.Integer:
                    return token.Value<int>();
                case JTokenType.Float:
                    return token.Value<double>();
                case JTokenType.String:
                    return token.Value<string>();
                case JTokenType.Boolean:
                    return token.Value<bool>();
                case JTokenType.Date:
                    return token.Value<DateTime>();
                case JTokenType.Guid:
                    return token.Value<Guid>();
                case JTokenType.TimeSpan:
                    return token.Value<TimeSpan>();
                default:
                    return "";
            }
        }

        private static Func<JsonDocument, object> CreateJPathDocumentExtractor(string binding)
        {
            try
            {
                var jpath = new RavenJPath(binding);

                return doc => GetTokenValue(doc.DataAsJson.SelectToken(jpath));
            }
            catch (Exception)
            {
                return doc => "<<Invalid Binding Expression>>";
            }
        }

        private class BoundValueExtractor : FrameworkElement
        {
            private readonly Binding templateBinding;

            private static readonly DependencyProperty ValueProperty =
                DependencyProperty.Register("Value", typeof(object), typeof(BoundValueExtractor),
                                            new PropertyMetadata(default(Type)));

            public BoundValueExtractor(Binding templateBinding)
            {
                this.templateBinding = templateBinding;
            }

            public object ExtractValue(object dataSource)
            {
                var binding = new Binding(templateBinding) { Source = dataSource };

                SetBinding(ValueProperty, binding);
                var value = GetValue(ValueProperty);
                ClearValue(ValueProperty);

                return value;
            }
        }
    }
}