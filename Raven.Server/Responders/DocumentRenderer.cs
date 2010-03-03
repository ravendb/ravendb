using System.Net;
using System.Text;
using Newtonsoft.Json.Linq;
using Raven.Database;

namespace Raven.Server.Responders
{
    public class DocumentRenderer
    {
        private const string ViewTemplateHeaderName = "Raven-View-Template";

        private readonly JsonDocument doc;
        private readonly HttpListenerContext context;
        private readonly DocumentDatabase database;

        public DocumentRenderer(JsonDocument doc, HttpListenerContext context, DocumentDatabase database)
        {
            this.doc = doc;
            this.context = context;
            this.database = database;
        }

        public void Render()
        {
            if (HasViewTemplate())
            {
                WriteAsTemplatedHtml();
            }
            else
            {
                WriteAsJson();
            }
        }

        private void WriteAsTemplatedHtml()
        {
            var output = GetHtml();

            context.Response.Headers["Content-Type"] = "text/html; charset=utf-8";
            context.Write(output);
        }

        private void WriteAsJson()
        {
            context.Response.Headers["Content-Type"] = "application/json; charset=utf-8";
            context.WriteData(doc.Data, doc.Metadata, doc.Etag);
        }

        private bool HasViewTemplate()
        {
            return doc.Metadata[ViewTemplateHeaderName] != null;
        }

        public string GetHtml()
        {
            var templateUrl = GetTemplateUrl(doc);

            var template = database.GetStatic(templateUrl);

            var utf8Encoding = new UTF8Encoding();

            return TemplateStart
                   + utf8Encoding.GetString(doc.Data)
                   + TemplateMiddle
                   + utf8Encoding.GetString(template.Data)
                   + TemplateEnd;
        }

        private static string GetTemplateUrl(JsonDocument doc)
        {
            var templateUrl = (string)((JValue)doc.Metadata[ViewTemplateHeaderName]).Value;
            templateUrl = templateUrl.Substring("/static/".Length);
            return templateUrl;
        }

        const string TemplateStart =
            @"<!DOCTYPE html PUBLIC '-//W3C//DTD XHTML 1.0 Transitional//EN' 'http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd'>
<html xmlns='http://www.w3.org/1999/xhtml'>
<head>

    <script type='text/javascript' src='/divan/js/jquery-1.4.2.min.js'></script>
    <script type='text/javascript' src='/divan/js/jquery-jtemplates.js'></script>

    <script type='text/javascript'>
        $(document).ready(function() {
            $('#results').setTemplateElement('template');
            $('#results').processTemplate(getData());
        });

        function getData() {
            return ";

        const string TemplateMiddle =
            @";
        }
    </script>
</head>
<body>
    <p style='display:none'>
    <textarea id='template' rows='0' cols='0'>
    <!--
";

        const string TemplateEnd =
            @"
-->
    </textarea>
    </p>
    
    <div id='results'></div>
</body>
</html>";
    }
}