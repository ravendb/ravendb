// -----------------------------------------------------------------------
//  <copyright file="DocumentHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.CodeAnalysis;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments;
using Raven.Server.Documents.Handlers.Processors.Attachments.Retired;
using Raven.Server.Documents.Handlers.Processors.Expiration;
using Raven.Server.Documents.TransactionMerger.Commands;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public sealed class RetiredAttachmentHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/attachments/retire/config", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetRetireConfig()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForGetRetireConfig(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/attachments/retire/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddRetireConfig()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForAddRetireConfig(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/attachments/retire", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task Get()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForGetRetiredAttachment(this, isDocument: true))
                await processor.ExecuteAsync();
        }

        //[RavenAction("/databases/*/attachments", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        //public async Task GetPost()
        //{
        //    using (var processor = new AttachmentHandlerProcessorForGetAttachment(this, isDocument: false))
        //        await processor.ExecuteAsync();
        //}

        [RavenAction("/databases/*/attachments/retire/bulk", "POST", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetAttachments()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForBulkRetiredAttachment(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/debug/attachments/retire/hash", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetHashCount()
        {
            using (var processor = new AttachmentHandlerProcessorForGetHashCount(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/debug/attachments/retire/metadata", "GET", AuthorizationStatus.ValidUser, EndpointType.Read, DisableOnCpuCreditsExhaustion = true)]
        public async Task GetAttachmentMetadataWithCounts()
        {
            using (var processor = new AttachmentHandlerProcessorForGetAttachmentMetadataWithCounts(this))
                await processor.ExecuteAsync();
        }

        //[RavenAction("/databases/*/attachments", "PUT", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        //public async Task Put()
        //{
        //    using (var processor = new AttachmentHandlerProcessorForPutAttachment(this))
        //        await processor.ExecuteAsync();
        //}

        [RavenAction("/databases/*/attachments/retire", "DELETE", AuthorizationStatus.ValidUser, EndpointType.Write, DisableOnCpuCreditsExhaustion = true)]
        public async Task Delete()
        {
            using (var processor = new RetiredAttachmentHandlerProcessorForDeleteRetiredAttachment(this))
            {
                await processor.ExecuteAsync();
            }
        }

    //    public sealed class MergedPutAttachmentCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    //    {
    //        public string DocumentId;
    //        public string Name;
    //        public LazyStringValue ExpectedChangeVector;
    //        public DocumentDatabase Database;
    //        public AttachmentDetails Result;
    //        public string ContentType;
    //        public Stream Stream;
    //        public string Hash;

    //        protected override long ExecuteCmd(DocumentsOperationContext context)
    //        {
    //            Result = Database.DocumentsStorage.AttachmentsStorage.PutAttachment(context, DocumentId, Name, ContentType, Hash, flags: AttachmentFlags.None, ExpectedChangeVector, Stream);
    //            return 1;
    //        }

    //        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    //        {
    //            return new MergedPutAttachmentCommandDto
    //            {
    //                DocumentId = DocumentId,
    //                Name = Name,
    //                ExpectedChangeVector = ExpectedChangeVector,
    //                ContentType = ContentType,
    //                Stream = Stream,
    //                Hash = Hash
    //            };
    //        }
    //    }

    //    internal sealed class MergedDeleteAttachmentCommand : MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>
    //    {
    //        public string DocumentId;
    //        public string Name;
    //        public LazyStringValue ExpectedChangeVector;
    //        public DocumentDatabase Database;

    //        protected override long ExecuteCmd(DocumentsOperationContext context)
    //        {
    //            Database.DocumentsStorage.AttachmentsStorage.DeleteAttachment(context, DocumentId, Name, ExpectedChangeVector, collectionName: out _);
    //            return 1;
    //        }

    //        public override IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, MergedTransactionCommand<DocumentsOperationContext, DocumentsTransaction>> ToDto(DocumentsOperationContext context)
    //        {
    //            return new MergedDeleteAttachmentCommandDto
    //            {
    //                DocumentId = DocumentId,
    //                Name = Name,
    //                ExpectedChangeVector = ExpectedChangeVector
    //            };
    //        }
    //    }
    //}

    //public sealed class MergedPutAttachmentCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, AttachmentHandler.MergedPutAttachmentCommand>
    //{
    //    public string DocumentId;
    //    public string Name;
    //    public LazyStringValue ExpectedChangeVector;
    //    public string ContentType;
    //    public Stream Stream;
    //    public string Hash;

    //    public AttachmentHandler.MergedPutAttachmentCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
    //    {
    //        return new AttachmentHandler.MergedPutAttachmentCommand
    //        {
    //            DocumentId = DocumentId,
    //            Name = Name,
    //            ExpectedChangeVector = ExpectedChangeVector,
    //            ContentType = ContentType,
    //            Stream = Stream,
    //            Hash = Hash,
    //            Database = database
    //        };
    //    }
    //}

    //internal sealed class MergedDeleteAttachmentCommandDto : IReplayableCommandDto<DocumentsOperationContext, DocumentsTransaction, AttachmentHandler.MergedDeleteAttachmentCommand>
    //{
    //    public string DocumentId;
    //    public string Name;
    //    public LazyStringValue ExpectedChangeVector;

    //    public AttachmentHandler.MergedDeleteAttachmentCommand ToCommand(DocumentsOperationContext context, DocumentDatabase database)
    //    {
    //        return new AttachmentHandler.MergedDeleteAttachmentCommand
    //        {
    //            DocumentId = DocumentId,
    //            Name = Name,
    //            ExpectedChangeVector = ExpectedChangeVector,
    //            Database = database
    //        };
    //    }
    }
}
