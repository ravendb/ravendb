using System.IO;
using System.Net;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Voron.Exceptions;

namespace Raven.Server.Files.Handlers
{
    public class FileHandler : FileSystemRequestHandler
    {
        [RavenAction("/fs/*/files", "GET", "/fs/{fileSystemName:string}/file?name={fileName:string}")]
        public async Task GetFile()
        {
            var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");
            var metadataOnly = GetBoolValueQueryString("metadata-only", required: false) ?? false;

            FilesOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            using (context.OpenReadTransaction())
            {
                var file = FileSystem.FilesStorage.Get(context, name);
                if (file == null)
                {
                    HttpContext.Response.StatusCode = (int) HttpStatusCode.NotFound;
                    return;
                }

                var etag = GetLongFromHeaders("If-None-Match");
                if (etag == file.Etag)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                    return;
                }

                HttpContext.Response.Headers[Constants.Headers.Etag] = "\"" + file.Etag + "\"";
                
                // Ensure that files are not cached at the browser side.
                HttpContext.Response.Headers["Cache-Control"] = "no-cache, no-store, must-revalidate";
                HttpContext.Response.Headers["Expires"] = "0";

                HttpContext.Response.Headers["Content-Type"] = file.ContentType;

                var stream = FileSystem.FilesStorage.GetStream(context, file.StreamIdentifier);
                if (stream == null)
                {
                    throw new FileNotFoundException("File is not uploaded yet");
                }

                var fileName = Path.GetFileName(name);
                HttpContext.Response.Headers["Content-Disposition"] = $"attachment; filename=\"{fileName}\"";
                // TODO: HttpContext.Response.Headers["Content-Range"] = ;

                // TODO: How should we return the metadata? As http headers?
                // Maybe we can split this to tow methods, GetMetadata and GetFile. 
                // And we can even leverage the document storage to hold the metadata as a document.
                // This way we also have indexing support built it.

                await stream.CopyToAsync(ResponseBodyStream());
            }
        }

        [RavenAction("/fs/*/files", "DELETE", "/fs/{fileSystemName:string}/files?name={fileName:string}")]
        public async Task Delete()
        {
            FilesOperationContext context;
            using (ContextPool.AllocateOperationContext(out context))
            {
                var name = GetQueryStringValueAndAssertIfSingleAndNotEmpty("name");

                var etag = GetLongFromHeaders("If-Match");

                var cmd = new MergedDeleteCommand
                {
                    Name = name,
                    FileSystem = FileSystem,
                    ExpectedEtag = etag
                };

                await FileSystem.TxMerger.Enqueue(cmd);

                cmd.ExceptionDispatchInfo?.Throw();

                NoContentStatus();
            }
        }

        private class MergedDeleteCommand : FilesTransactionsMerger.MergedTransactionCommand
        {
            public string Name;
            public long? ExpectedEtag;
            public FileSystem FileSystem;
            public ExceptionDispatchInfo ExceptionDispatchInfo;

            public override void Execute(FilesOperationContext context)
            {
                try
                {
                    FileSystem.FilesStorage.Delete(context, Name, ExpectedEtag);
                }
                catch (ConcurrencyException e)
                {
                    ExceptionDispatchInfo = ExceptionDispatchInfo.Capture(e);
                }
            }
        }
    }
}