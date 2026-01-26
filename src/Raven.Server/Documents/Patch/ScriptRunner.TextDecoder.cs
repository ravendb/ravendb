using System.Text;
using Jint;
using Jint.Native;

namespace Raven.Server.Documents.Patch;

public sealed partial class ScriptRunner
{
    public sealed partial class SingleRun
    {
        private void InitializeTextDecoder()
        {
            ScriptEngine.SetClrFunc("TextDecoder_Decode", TextDecoder_Decode);
            ScriptEngine.Execute(
                """
                globalThis.TextDecoder = function(label, options) {
                    var encoding = 'utf-8';
                    if (typeof label === 'string' && label.length > 0) encoding = label;
                    if (encoding.toLowerCase() !== 'utf-8' && encoding.toLowerCase() !== 'utf8')
                        throw new Error("TextDecoder: encoding '" + encoding + "' is not supported. Only 'utf-8' is supported.");
                    if (options != null && Object.keys(options).length > 0)
                        throw new Error("TextDecoder: options are not supported.");
                    this.encoding = encoding;
                };
                globalThis.TextDecoder.prototype.decode = function(input) {
                    return TextDecoder_Decode(input);
                };
                """);
        }

        private JsValue TextDecoder_Decode(JsValue self, JsValue[] args)
        {
            if (args.Length == 0 || args[0].IsUndefined())
                return JsString.Empty;
            const string signature = "TextDecoder.decode";
            byte[] bytes = GetBytesFromJsValue(args[0], signature);
            return Encoding.UTF8.GetString(bytes);
        }
    }
}
