// -----------------------------------------------------------------------
//  <copyright file="AdminFSController.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Threading.Tasks;
using System.Web.Http;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Database.Extensions;
using Raven.Database.Server.Controllers.Admin;
using Raven.Database.Server.Tenancy;
using Raven.Json.Linq;

namespace Raven.Database.Server.RavenFS.Controllers
{
    public class AdminFileSystemController : BaseAdminController
    {
        [HttpPut]
        [Route("admin/fs/{*id}")]
        public async Task<HttpResponseMessage> FileSystemPut(string id, bool update = false)
        {
			
			MessageWithStatusCode fileSystemNameFormat = CheckNameFormat(id, Database.Configuration.FileSystemDataDirectory);
			if (fileSystemNameFormat.Message != null)
			{
				return GetMessageWithString(fileSystemNameFormat.Message, fileSystemNameFormat.ErrorCode);
			}

            var docKey = "Raven/FileSystems/" + id;
           
            // There are 2 possible ways to call this put. We either want to update a filesystem configuration or we want to create a new one.            
            if (!update)
            {
                // As we are not updating, we should fail when the filesystem already exists.
                var existingFilesystem = Database.Documents.Get(docKey, null);
                if (existingFilesystem != null)                   
                    return GetEmptyMessage(HttpStatusCode.Conflict);
            }

			var fsDoc = await ReadJsonObjectAsync<DatabaseDocument>();
			FileSystemsLandlord.Protect(fsDoc);
			var json = RavenJObject.FromObject(fsDoc);
            json.Remove("Id");

            Database.Documents.Put(docKey, null, json, new RavenJObject(), null);

            return GetEmptyMessage(HttpStatusCode.Created);
        }

		[HttpDelete]
		[Route("admin/fs/{*id}")]
		public HttpResponseMessage FileSystemDelete(string id)
		{
			bool result;
			var isHardDeleteNeeded = bool.TryParse(InnerRequest.RequestUri.ParseQueryString()["hard-delete"], out result) && result;

			var message = DeleteFileSystem(id, isHardDeleteNeeded);
			if (message.ErrorCode != HttpStatusCode.OK)
			{
				return GetMessageWithString(message.Message, message.ErrorCode);
			}

			return GetEmptyMessage();
		}

		[HttpDelete]
		[Route("admin/fs/filesystem-batch-delete")]
		public HttpResponseMessage FileSystemBatchDelete()
		{
			string[] fileSystemsToDelete = GetQueryStringValues("fileSystemsIds");
			if (fileSystemsToDelete == null)
			{
				return GetMessageWithString("No file systems to delete!", HttpStatusCode.BadRequest);
			}

			bool result;
			var isHardDeleteNeeded = bool.TryParse(InnerRequest.RequestUri.ParseQueryString()["hard-delete"], out result) && result;
			var successfullyDeletedDatabase = new List<string>();

			fileSystemsToDelete.ForEach(fileSystemId =>
			{
				var message = DeleteFileSystem(fileSystemId, isHardDeleteNeeded);
				if (message.ErrorCode == HttpStatusCode.OK)
				{
					successfullyDeletedDatabase.Add(fileSystemId);
				}

			});

			return GetMessageWithObject(successfullyDeletedDatabase.ToArray());
		}

		[HttpPost]
		[Route("admin/fs/{*id}")]
		public HttpResponseMessage FileSystemToggleDisable(string id, bool isSettingDisabled)
		{
			var message = ToggeleFileSystemDisabled(id, isSettingDisabled);
			if (message.ErrorCode != HttpStatusCode.OK)
			{
				return GetMessageWithString(message.Message, message.ErrorCode);
			}

			return GetEmptyMessage();
		}

		[HttpPost]
		[Route("admin/fs/filesystem-batch-toggle-disable")]
		public HttpResponseMessage FileSystemBatchToggleDisable(bool isSettingDisabled)
		{
			string[] databasesToToggle = GetQueryStringValues("ids");
			if (databasesToToggle == null)
			{
				return GetMessageWithString("No file systems to toggle!", HttpStatusCode.BadRequest);
			}

			var successfullyToggledFileSystems = new List<string>();

			databasesToToggle.ForEach(fileSystemId =>
			{
				var message = ToggeleFileSystemDisabled(fileSystemId, isSettingDisabled);
				if (message.ErrorCode == HttpStatusCode.OK)
				{
					successfullyToggledFileSystems.Add(fileSystemId);
				}

			});

			return GetMessageWithObject(successfullyToggledFileSystems.ToArray());
		}

		private MessageWithStatusCode DeleteFileSystem(string fileSystemId, bool isHardDeleteNeeded)
		{
			//get configuration even if the file system is disabled
			var configuration = FileSystemsLandlord.CreateTenantConfiguration(fileSystemId, true);

			if (configuration == null)
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "File system wasn't found" };

			var docKey = "Raven/FileSystems/" + fileSystemId;
			Database.Documents.Delete(docKey, null, null);

			if (isHardDeleteNeeded)
			{
				IOExtensions.DeleteDirectory(configuration.FileSystemDataDirectory);
				if (configuration.IndexStoragePath != null)
					IOExtensions.DeleteDirectory(configuration.IndexStoragePath);
				if (configuration.JournalsStoragePath != null)
					IOExtensions.DeleteDirectory(configuration.JournalsStoragePath);
			}

			return new MessageWithStatusCode();
		}

		private MessageWithStatusCode ToggeleFileSystemDisabled(string fileSystemId, bool isSettingDisabled)
		{
			var docKey = "Raven/FileSystems/" + fileSystemId;
			var document = Database.Documents.Get(docKey, null);
			if (document == null)
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.NotFound, Message = "File system " + fileSystemId + " wasn't found" };

			var fsDoc = document.DataAsJson.JsonDeserialization<DatabaseDocument>();
			if (fsDoc.Disabled == isSettingDisabled)
			{
				string state = isSettingDisabled ? "disabled" : "enabled";
				return new MessageWithStatusCode { ErrorCode = HttpStatusCode.BadRequest, Message = "File system " + fileSystemId + " is already " + state };
			}

			FileSystemsLandlord.Unprotect(fsDoc);
			fsDoc.Disabled = !fsDoc.Disabled;
			var json = RavenJObject.FromObject(fsDoc);
			json.Remove("Id");
			Database.Documents.Put(docKey, document.Etag, json, new RavenJObject(), null);

			return new MessageWithStatusCode();
		}
    }
}