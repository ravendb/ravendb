using Raven.Server.Documents.Patch;

namespace Raven.Server.Documents.Indexes.Static;

public class JavaScriptIndexHelper
{
    public static string GetMapCode()
    {
        return @"
function map(name, lambda) {
    var map = {
        collection: name,
        method: lambda,
        moreArgs: Array.prototype.slice.call(arguments, 2)
    };
    globalDefinition.maps.push(map);
}";
    }

    public static void RegisterJavaScriptUtils(IJavaScriptUtilsClearance javaScriptUtils)
    {
        var scope = CurrentIndexingScope.Current;
        scope.RegisterJavaScriptUtils(javaScriptUtils);
    }
}

public interface IJavaScriptIndex<T>
    where T : struct, IJsHandle<T>
{
    public T GetDocumentId(T self, T[] args);
    public T AttachmentsFor(T self, T[] args);
    public T MetadataFor(T self, T[] args);
    public T TimeSeriesNamesFor(T self, T[] args);
    public T CounterNamesFor(T self, T[] args);
    public T LoadAttachment(T self, T[] args);
    public T LoadAttachments(T self, T[] args);
}
