using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Text.RegularExpressions;
using Raven.Server.Config;
using Raven.Server.Config.Categories;

namespace Raven.Server.Web
{
    public class HtmlUtil
    {
        public static string RenderPlaceholders(string html, IDictionary<string, string> substitutions)
        {
            return Regex.Replace(html, @"{{\s*(\w+)\s*}}", match => substitutions[match.Groups[1].Value], RegexOptions.Multiline);
        }

        private const string UnsafePageHtmlResource = "Raven.Server.Web.Assets.Unsafe.html";
        
        private const string AuthErrorPageHtmlResource = "Raven.Server.Web.Assets.AuthError.html";

        private static string _unsafePageRenderedHtml;

        public static string RenderUnsafePage()
        {
            if (_unsafePageRenderedHtml == null)
            {
                using (var reader = new StreamReader(
                    typeof(RavenServer).GetTypeInfo().Assembly.GetManifestResourceStream(UnsafePageHtmlResource)))
                {
                    var html = reader.ReadToEnd();

                    var unsecuredAccessFlagsHtml = GetUnsecuredAccessFlagsHtml();

                    var certPathOptDescription = typeof(SecurityConfiguration).GetProperty("CertificatePath").GetCustomAttribute<DescriptionAttribute>().Description;

                    var certExecOptDescription = typeof(SecurityConfiguration).GetProperty("CertificateExec")
                        .GetCustomAttribute<DescriptionAttribute>().Description;

                    _unsafePageRenderedHtml = RenderPlaceholders(html, new Dictionary<string, string>()
                    {
                        {"UNSECURED_ACCESS_ALLOWED_KEY", RavenConfiguration.GetKey(x => x.Security.UnsecuredAccessAllowed)},
                        {"SERVER_URL_KEY", RavenConfiguration.GetKey(x => x.Core.ServerUrls)},
                        {"UNSECURED_ACCESS_ALLOWED_FLAGS", unsecuredAccessFlagsHtml},
                        {"CERTIFICATE_EXEC_KEY", RavenConfiguration.GetKey(x => x.Security.CertificateExecLoad)},
                        {"CERTIFICATE_PATH_KEY", RavenConfiguration.GetKey(x => x.Security.CertificatePath)},
                        {"CERTIFICATE_EXEC_DESCRIPTION", certExecOptDescription},
                        {"CERTIFICATE_PATH_DESCRIPTION", certPathOptDescription}
                    });
                }
            }

            return _unsafePageRenderedHtml;
        }

        private static string GetUnsecuredAccessFlagsHtml()
        {
            var flagsSpans = Enum.GetNames(typeof(UnsecuredAccessAddressRange))
                .Select(x => $"<span class='text-warning'>{x}</span>");

            var unsecuredAccessFlagsHtml = string.Join("&nbsp;<strong>,</strong>&nbsp;", flagsSpans);
            return unsecuredAccessFlagsHtml;
        }
        
        public static string RenderStudioAuthErrorPage(string reason)
        {
            using (var reader = new StreamReader(
                typeof(RavenServer).GetTypeInfo().Assembly.GetManifestResourceStream(AuthErrorPageHtmlResource)))
            {
                var html = reader.ReadToEnd();

                return RenderPlaceholders(html, new Dictionary<string, string>
                {
                    {"AUTH_ERROR", WebUtility.HtmlEncode(reason)}
                });
            }
        }
    }
}
