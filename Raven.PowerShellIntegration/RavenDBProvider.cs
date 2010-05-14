using System;
using System.Linq;
using System.Management.Automation;
using System.Management.Automation.Provider;
using System.Text;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Database;
using Raven.Database.Indexing;

namespace Raven.PowerShellIntegration
{
	[CmdletProvider("RavenDB", ProviderCapabilities.None)]
	public class RavenDBProvider : ContainerCmdletProvider, IContentCmdletProvider
	{
		private string pathSeparator = "\\";

		#region Public Enumerations

		public enum PathType
		{
			Database,
			Documents,
			Document,
			Indexes,
			Index,
			Invalid
		} ;

		#endregion

		#region Drive Manipulation

		/// <summary>
		/// 	Create a new drive.  Create a connection to the database and set
		/// 	the Database property in the PSDriveInfo.
		/// </summary>
		/// <param name = "drive">
		/// 	The path to the RavenDB data directory.
		/// </param>
		/// <returns>The added drive.</returns>
		protected override PSDriveInfo NewDrive(PSDriveInfo drive)
		{
			// check if drive object is null
			if (drive == null)
			{
				WriteError(new ErrorRecord(
				           	new ArgumentNullException("drive"),
				           	"NullDrive",
				           	ErrorCategory.InvalidArgument,
				           	null)
					);

				return null;
			}

			// check if drive root is not null or empty
			if (String.IsNullOrEmpty(drive.Root))
			{
				WriteError(new ErrorRecord(
				           	new ArgumentException("drive.Root"),
				           	"NoRoot",
				           	ErrorCategory.InvalidArgument,
				           	drive)
					);

				return null;
			}


			var ravenDBPSDriveInfo = new RavenDBPSDriveInfo(drive);
			var db = new DocumentDatabase(new RavenConfiguration {DataDirectory = drive.Root});

			ravenDBPSDriveInfo.Database = db;

			return ravenDBPSDriveInfo;
		}

		// NewDrive

		/// <summary>
		/// 	Removes a drive from the provider.
		/// </summary>
		/// <param name = "drive">The drive to remove.</param>
		/// <returns>The drive removed.</returns>
		protected override PSDriveInfo RemoveDrive(PSDriveInfo drive)
		{
			// check if drive object is null
			if (drive == null)
			{
				WriteError(new ErrorRecord(
				           	new ArgumentNullException("drive"),
				           	"NullDrive",
				           	ErrorCategory.InvalidArgument,
				           	drive)
					);

				return null;
			}

			// dispose database on drive
			var ravenDBPSDriveInfo = drive as RavenDBPSDriveInfo;

			if (ravenDBPSDriveInfo == null)
			{
				return null;
			}

			ravenDBPSDriveInfo.Database.Dispose();

			return ravenDBPSDriveInfo;
		}

		// RemoveDrive

		#endregion Drive Manipulation

		#region Item Methods

		/// <summary>
		/// 	Retrieves an item using the specified path.
		/// </summary>
		/// <param name = "path">The path to the item to return.</param>
		protected override void GetItem(string path)
		{
			// check if the path represented is a drive
			if (PathIsDrive(path))
			{
				WriteItemObject(this.PSDriveInfo, path, true);
				return;
			}

			// Get path type and document id/index name if applicable
			string pathTypeValue;
			Guid? etag;

			var type = GetNamesFromPath(path, out pathTypeValue, out etag);

			if (type == PathType.Document)
			{
				var db = PSDriveInfo as RavenDBPSDriveInfo;
				JsonDocument document;
				if (db == null)
					document = null;
				else
				    document = db.Database.Get(pathTypeValue, null);
				var str = document.DataAsJson.ToString(Formatting.Indented);

				WriteItemObject(str, path, true);
			}
			else if (type == PathType.Index)
			{
				var db = this.PSDriveInfo as RavenDBPSDriveInfo;
				string index;
				if (db == null)
					index = null;
				else
				{
					var indexDefinition = db.Database.IndexDefinitionStorage.GetIndexDefinition(pathTypeValue);
					if (indexDefinition != null)
						index = indexDefinition.Map + Environment.NewLine + indexDefinition.Reduce;
					else
						index = null;
				}
				WriteItemObject(index, path, true);
			}
			else
			{
				ThrowTerminatingInvalidPathException(path);
			}
		}

		/// <summary>
		/// 	Set the content of a document or an index based on the path parameter
		/// </summary>
		/// <param name = "path">Specifies the path to the document or index whose values are to be set.</param>
		/// <param name = "values">The value to be set</param>
		protected override void SetItem(string path, object value)
		{
			// Get path type and document id/index name if applicable
			string pathTypeValue;
			Guid? etag;

			var type = GetNamesFromPath(path, out pathTypeValue, out etag);

			if (type == PathType.Document)
			{
				JObject doc = null;
				try
				{
					doc = JObject.Parse(value.ToString());
				}
				catch (Exception)
				{
				}

				if (doc == null)
				{
					WriteError(new ErrorRecord(new ArgumentException(
					                           	"Invalid JSON"), "",
					                           ErrorCategory.InvalidArgument, path));
					return;
				}

				if (!string.IsNullOrEmpty(pathTypeValue) && etag.HasValue)
				{
					var db = this.PSDriveInfo as RavenDBPSDriveInfo;
                    db.Database.Put(pathTypeValue, etag, doc, new JObject(),
                                           null);
				}
				else
				{
					WriteError(new ErrorRecord(new ArgumentException(
					                           	"Document path must have an ID and an ETag"), "",
					                           ErrorCategory.InvalidArgument, path));
				}
			}
			else if (type == PathType.Index)
			{
				if (!string.IsNullOrEmpty(pathTypeValue))
				{
					var db = this.PSDriveInfo as RavenDBPSDriveInfo;
					if (db.Database.IndexDefinitionStorage.IndexNames.Contains(pathTypeValue))
						db.Database.PutIndex(pathTypeValue, (IndexDefinition)value);
					else
						WriteError(new ErrorRecord(new ArgumentException(
						                           	"Index does not exist."), "",
						                           ErrorCategory.InvalidArgument, path));
				}
				else
				{
					WriteError(new ErrorRecord(new ArgumentException(
					                           	"Index path must have a name"), "",
					                           ErrorCategory.InvalidArgument, path));
				}
			}
			else
			{
				WriteError(new ErrorRecord(new NotSupportedException(
				                           	"SetNotSupported"), "",
				                           ErrorCategory.InvalidOperation, path));
			}
		}

		// SetItem

		/// <summary>
		/// 	Test to see if the specified item exists.
		/// </summary>
		/// <param name = "path">The path to the item to verify.</param>
		/// <returns>True if the item is found.</returns>
		protected override bool ItemExists(string path)
		{
			// check if the path represented is a drive
			if (PathIsDrive(path))
			{
				return true;
			}

			string pathTypeValue;
			Guid? etag;

			var type = GetNamesFromPath(path, out pathTypeValue, out etag);

			if (type == PathType.Document)
			{
				var db = this.PSDriveInfo as RavenDBPSDriveInfo;
				JsonDocument document;
				if (db == null)
					document = null;
				else
				    document = db.Database.Get(pathTypeValue, null);

				return document != null;
			}
		    if (type == PathType.Documents)
		        return true;
		    if (type == PathType.Index)
		    {
		        var db = this.PSDriveInfo as RavenDBPSDriveInfo;
		        string index;
		        if (db == null)
		            index = null;
		        else
		        {
		        	var indexDefinition = db.Database.IndexDefinitionStorage.GetIndexDefinition(pathTypeValue);
					if (indexDefinition != null)
						index = indexDefinition.Map + Environment.NewLine + indexDefinition.Reduce;
					else
						index = null;
		        }

		    	return index != null;
		    }
		    if (type == PathType.Indexes)
		        return true;
		    ThrowTerminatingInvalidPathException(path);

		    return false;
		}

		// ItemExists

		/// <summary>
		/// 	Test to see if the specified path is syntactically valid.
		/// </summary>
		/// <param name = "path">The path to validate.</param>
		/// <returns>True if the specified path is valid.</returns>
		protected override bool IsValidPath(string path)
		{
			var result = true;

			// check if the path is null or empty
			if (String.IsNullOrEmpty(path))
			{
				result = false;
			}

			// convert all separators in the path to a uniform one
			path = NormalizePath(path);

			// split the path into individual chunks
			var pathChunks = path.Split(pathSeparator.ToCharArray());

			foreach (var pathChunk in pathChunks)
			{
				if (pathChunk.Length == 0)
				{
					result = false;
				}
			}
			return result;
		}

		// IsValidPath

		#endregion Item Overloads

		#region Container Overloads

		/// <summary>
		/// 	Return either the documents or indexes in the database
		/// </summary>
		/// <param name = "path">The path to the parent</param>
		/// <param name = "recurse">Ignored
		/// </param>
		protected override void GetChildItems(string path, bool recurse)
		{
			var db = this.PSDriveInfo as RavenDBPSDriveInfo;

			// If path represented is a drive then the children will be all indexes and tables
			if (PathIsDrive(path))
			{
				WriteItemObject("documents\\", "documents", true);
				foreach (JObject doc in db.Database.GetDocuments(0, int.MaxValue, null))
				{
					WriteItemObject('\t' + doc["@metadata"]["@id"].Value<string>(), path, false);
				}
				WriteItemObject("indexes\\", "indexes", true);
				foreach (JObject index in db.Database.GetIndexes(0, int.MaxValue))
				{
					WriteItemObject('\t' + index["name"].Value<string>(), path, false);
				}
			}
			else
			{
				string pathTypeValue;
				Guid? etag;

				var type = GetNamesFromPath(path, out pathTypeValue, out etag);

				if (type == PathType.Documents)
				{
					foreach (JObject doc in db.Database.GetDocuments(0, int.MaxValue, null))
					{
						WriteItemObject(doc["@metadata"]["@id"].Value<string>(), path, false);
					}
				}
				else if (type == PathType.Indexes)
				{
					foreach (JObject index in db.Database.GetIndexes(0, int.MaxValue))
					{
						WriteItemObject(index["name"].Value<string>(), path, false);
					}
				}
				else
				{
					// In this case, the path specified is not valid
					var message = new StringBuilder("Path must represent either a document or an index.");
					message.Append(path);

					throw new ArgumentException(message.ToString());
				}
			}
		}

		/// <summary>
		/// 	Return the keys/names of documents and/or indexes
		/// </summary>
		/// <param name = "path">The root path.</param>
		/// <param name = "returnContainers">Not used.</param>
		protected override void GetChildNames(string path, ReturnContainers returnContainers)
		{
			var db = this.PSDriveInfo as RavenDBPSDriveInfo;

			// If path represented is a drive then the children will be all indexes and tables
			if (PathIsDrive(path))
			{
				foreach (JObject doc in db.Database.GetDocuments(0, int.MaxValue,null))
				{
					WriteItemObject(doc["@metadata"]["@id"].Value<string>(), path, false);
				}
				foreach (JObject index in db.Database.GetIndexes(0, int.MaxValue))
				{
					WriteItemObject(index["name"].Value<string>(), path, false);
				}
			}
			else
			{
				string pathTypeValue;
				Guid? etag;

				var type = GetNamesFromPath(path, out pathTypeValue, out etag);

				if (type == PathType.Documents)
				{
					foreach (JObject doc in db.Database.GetDocuments(0, int.MaxValue,null))
					{
						WriteItemObject(doc["@metadata"]["@id"].Value<string>(), path, false);
					}
				}
				else if (type == PathType.Indexes)
				{
					foreach (JObject index in db.Database.GetIndexes(0, int.MaxValue))
					{
						WriteItemObject(index["name"].Value<string>(), path, false);
					}
				}
				else
				{
					// In this case, the path specified is not valid
					var message = new StringBuilder("Path must represent either documents or indexes.");
					message.Append(path);

					throw new ArgumentException(message.ToString());
				}
			}
		}

		// GetChildNames

		/// <summary>
		/// 	Determines if the specified path has child items.
		/// </summary>
		/// <param name = "path">The path to examine.</param>
		/// <returns>
		/// 	True if the specified path has child items.
		/// </returns>
		protected override bool HasChildItems(string path)
		{
			if (PathIsDrive(path))
			{
				return true;
			}

			return (ChunkPath(path).Length == 1);
		}

		// HasChildItems

		/// <summary>
		/// 	Creates a new item at the specified path.
		/// </summary>
		/// <param name = "path">
		/// 	The path to the new item.
		/// </param>
		/// <param name = "type">
		/// 	Type for the object to create. "Document" or "Index"
		/// </param>
		/// <param name = "newItemValue">
		/// 	Object for creating new instance of a type at the specified path.
		/// </param>
		protected override void NewItem(string path, string type, object newItemValue)
		{
			// Get path type and document id/index name if applicable
			string pathTypeValue;
			Guid? etag;

			var pathType = GetNamesFromPath(path, out pathTypeValue, out etag);

			if (type.ToLower() != "index" && type.ToLower() != "document")
			{
				WriteError(new ErrorRecord
				           	(new ArgumentException("Type must be either a document or index"),
				           	 "CannotCreateSpecifiedObject",
				           	 ErrorCategory.InvalidArgument,
				           	 path
				           	)
					);

				throw new ArgumentException("This provider can only create items of type \"document\" or \"index\"");
			}
			if (pathType == PathType.Database)
			{
			}
			else if ((pathType == PathType.Documents && type.ToLower() == "document") ||
				(pathType == PathType.Database && type.ToLower() == "document"))
			{
				JObject doc = null;
				try
				{
					doc = JObject.Parse(newItemValue.ToString());
				}
				catch (Exception)
				{
				}

				if (doc == null)
				{
					WriteError(new ErrorRecord(new ArgumentException(
					                           	"Invalid JSON"), "",
					                           ErrorCategory.InvalidArgument, path));
					return;
				}

				var db = this.PSDriveInfo as RavenDBPSDriveInfo;
                db.Database.Put(null, Guid.Empty, doc, new JObject(),
                                           null);
			}
			else if (pathType == PathType.Index && type.ToLower() == "index")
			{
				if (!string.IsNullOrEmpty(pathTypeValue))
				{
					var db = this.PSDriveInfo as RavenDBPSDriveInfo;
					if (!db.Database.IndexDefinitionStorage.IndexNames.Contains(pathTypeValue))
						db.Database.PutIndex(pathTypeValue, (IndexDefinition)newItemValue);
					else
						WriteError(new ErrorRecord(new ArgumentException(
						                           	"Index already exists."), "",
						                           ErrorCategory.InvalidArgument, path));
				}
				else
				{
					WriteError(new ErrorRecord(new ArgumentException(
					                           	"Index path must have a name"), "",
					                           ErrorCategory.InvalidArgument, path));
				}
			}
			else
			{
				WriteError(new ErrorRecord(new NotSupportedException(
				                           	"Create not supported for the specified path"), "",
				                           ErrorCategory.InvalidOperation, path));
			}
		}

		// NewItem

		/// <summary>
		/// 	Copies an item at the specified path to the location specified
		/// </summary>
		/// <param name = "path">
		/// 	Path of the item to copy
		/// </param>
		/// <param name = "copyPath">
		/// 	Path of the item to copy to
		/// </param>
		/// <param name = "recurse">
		/// 	Tells the provider to recurse subcontainers when copying
		/// </param>
		protected override void CopyItem(string path, string copyPath, bool recurse)
		{
			throw new NotImplementedException();
		}

		//CopyItem

		/// <summary>
		/// 	Removes (deletes) the item at the specified path
		/// </summary>
		/// <param name = "path">
		/// 	The path to the item to remove.
		/// </param>
		/// <param name = "recurse">
		/// 	Ignored
		/// </param>
		/// <remarks>
		/// 	There are no elements in this store which are hidden from the user.
		/// 	Hence this method will not check for the presence of the Force
		/// 	parameter
		/// </remarks>
		protected override void RemoveItem(string path, bool recurse)
		{
			// Get path type and document id/index name if applicable
			string pathTypeValue;
			Guid? etag;

			var type = GetNamesFromPath(path, out pathTypeValue, out etag);

			if (type == PathType.Document)
			{
				if (!string.IsNullOrEmpty(pathTypeValue) && etag.HasValue)
				{
					var db = this.PSDriveInfo as RavenDBPSDriveInfo;
                    db.Database.Delete(pathTypeValue, etag,
                                           null);
				}
				else
				{
					WriteError(new ErrorRecord(new ArgumentException(
					                           	"Document path must have an ID and an ETag"), "",
					                           ErrorCategory.InvalidArgument, path));
				}
			}
			else if (type == PathType.Index)
			{
				if (!string.IsNullOrEmpty(pathTypeValue))
				{
					var db = this.PSDriveInfo as RavenDBPSDriveInfo;
					db.Database.DeleteIndex(pathTypeValue);
				}
				else
				{
					WriteError(new ErrorRecord(new ArgumentException(
					                           	"Index path must have a name"), "",
					                           ErrorCategory.InvalidArgument, path));
				}
			}
			else
			{
				WriteError(new ErrorRecord(new NotSupportedException(
				                           	"DeleteNotSupported"), "",
				                           ErrorCategory.InvalidOperation, path));
			}
		}

		// RemoveItem

		#endregion Container Overloads

		#region Content Methods

		/// <summary>
		/// 	Clear the contents at the specified location.
		/// </summary>
		/// <param name = "path">The path to the content to clear.</param>
		public void ClearContent(string path)
		{
			var db = this.PSDriveInfo as RavenDBPSDriveInfo;

			// If path represented is a drive then the children will be all indexes and tables
			if (PathIsDrive(path))
			{
				foreach (JObject doc in db.Database.GetDocuments(0, int.MaxValue,null))
				{
                    db.Database.Delete(doc["@metadata"]["@id"].Value<string>(), doc["@metadata"]["@etag"].Value<Guid?>(),
                                           null);
				}
				foreach (JObject index in db.Database.GetIndexes(0, int.MaxValue))
				{
					db.Database.DeleteIndex(index["@name"].Value<string>());
				}
			}
			else
			{
				string pathTypeValue;
				Guid? etag;

				var type = GetNamesFromPath(path, out pathTypeValue, out etag);

				if (type == PathType.Documents)
				{
                    foreach (JObject doc in db.Database.GetDocuments(0, int.MaxValue, null))
					{
						db.Database.Delete(doc["@metadata"]["@id"].Value<string>(),
						                   doc["@metadata"]["@etag"].Value<Guid?>(),
                                           null);
					}
				}
				else if (type == PathType.Indexes)
				{
					foreach (JObject index in db.Database.GetIndexes(0, int.MaxValue))
					{
						db.Database.DeleteIndex(index["@name"].Value<string>());
					}
				}
				else
				{
					// In this case, the path specified is not valid
					var message = new StringBuilder("Path must represent either documents or indexes.");
					message.Append(path);

					throw new ArgumentException(message.ToString());
				}
			}
		}

		// ClearContent

		/// <summary>
		/// 	Not implemented.
		/// </summary>
		/// <param name = "path"></param>
		/// <returns></returns>
		public object ClearContentDynamicParameters(string path)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// 	Get a reader at the path specified.
		/// </summary>
		/// <param name = "path">The path from which to read.</param>
		/// <returns>A content reader used to read the data.</returns>
		public IContentReader GetContentReader(string path)
		{
			string pathTypeValue;
			Guid? etag;

			var type = GetNamesFromPath(path, out pathTypeValue, out etag);

			if (type == PathType.Documents)
			{
				return new RavenDBContentReader(((RavenDBPSDriveInfo) this.PSDriveInfo).Database, true);
			}
			else if (type == PathType.Indexes)
			{
				return new RavenDBContentReader(((RavenDBPSDriveInfo) this.PSDriveInfo).Database, false);
			}

			throw new InvalidOperationException("Contents can be obtained only for documents and indexes");
		}

		// GetContentReader

		/// <summary>
		/// 	Not implemented.
		/// </summary>
		/// <param name = "path"></param>
		/// <returns></returns>
		public object GetContentReaderDynamicParameters(string path)
		{
			return null;
		}

		/// <summary>
		/// 	Get an object used to write content.
		/// </summary>
		/// <param name = "path">The root path at which to write.</param>
		/// <returns>A content writer for writing.</returns>
		public IContentWriter GetContentWriter(string path)
		{
			throw new NotImplementedException();
		}

		/// <summary>
		/// 	Not implemented.
		/// </summary>
		/// <param name = "path"></param>
		/// <returns></returns>
		public object GetContentWriterDynamicParameters(string path)
		{
			return null;
		}

		#endregion Content Methods

		#region Helper Methods

		/// <summary>
		/// 	Checks if a given path is actually a drive name.
		/// </summary>
		/// <param name = "path">The path to check.</param>
		/// <returns>
		/// 	True if the path given represents a drive, false otherwise.
		/// </returns>
		private bool PathIsDrive(string path)
		{
			// Remove the drive name and first path separator.  If the 
			// path is reduced to nothing, it is a drive. Also if its
			// just a drive then there wont be any path separators
			if (String.IsNullOrEmpty(
				path.Replace(this.PSDriveInfo.Root, "")) ||
					String.IsNullOrEmpty(
						path.Replace(this.PSDriveInfo.Root + pathSeparator, ""))
				)
			{
				return true;
			}
			else
			{
				return false;
			}
		}

		// PathIsDrive
		/// <summary>
		/// 	Chunks the path and returns the path type & document id or index name mentioned in the path
		/// </summary>
		/// <param name = "path">Path to chunk and obtain information</param>
		/// <param name = "pathTypeName">Name of the document or index defined by the path</param>
		/// <returns>what the path represents</returns>
		public PathType GetNamesFromPath(string path, out string pathTypeValue, out Guid? etag)
		{
			var retVal = PathType.Invalid;
			pathTypeValue = null;
			etag = null;

			// Check if the path specified is a drive
			if (PathIsDrive(path))
			{
				return PathType.Database;
			}

			// chunk the path into parts
			var pathChunks = ChunkPath(path);

			switch (pathChunks.Length)
			{
				case 1:
				{
					var name = pathChunks[0];
					if (name.ToLower() == "documents")
						retVal = PathType.Documents;
					else if (name.ToLower() == "indexes")
						retVal = PathType.Indexes;
					else
					{
						retVal = PathType.Document;
						pathTypeValue = pathChunks[0];
					}
				}
					break;

				case 2:
				{
					var name = pathChunks[0];
					if (name.ToLower() == "documents")
						retVal = PathType.Document;
					else if (name.ToLower() == "indexes")
						retVal = PathType.Index;

					pathTypeValue = pathChunks[1];
				}
					break;
				case 3:
				{
					var name = pathChunks[0];
					if (name.ToLower() == "documents")
					{
						retVal = PathType.Document;

						pathTypeValue = pathChunks[1];

						Guid etagNN;
						if (Guid.TryParse(pathChunks[2], out etagNN))
							etag = etagNN;
						else
						{
							WriteError(new ErrorRecord(
							           	new ArgumentException("Invalid ETag"),
							           	"PathNotValid",
							           	ErrorCategory.InvalidArgument,
							           	path));
						}
					}
					break;
				}
				default:
				{
					WriteError(new ErrorRecord(
					           	new ArgumentException("The path supplied has too many segments"),
					           	"PathNotValid",
					           	ErrorCategory.InvalidArgument,
					           	path));
				}
					break;
			}
			return retVal;
		}

		// GetNamesFromPath

		/// <summary>
		/// 	Breaks up the path into individual elements.
		/// </summary>
		/// <param name = "path">The path to split.</param>
		/// <returns>An array of path segments.</returns>
		private string[] ChunkPath(string path)
		{
			// Normalize the path before splitting
			var normalPath = NormalizePath(path);

			// Return the path with the drive name and first path 
			// separator character removed, split by the path separator.
			var pathNoDrive = normalPath.Replace(this.PSDriveInfo.Root
				+ pathSeparator, "");

			return pathNoDrive.Split(pathSeparator.ToCharArray());
		}

		// ChunkPath

		/// <summary>
		/// 	Adapts the path, making sure the correct path separator
		/// 	character is used.
		/// </summary>
		/// <param name = "path"></param>
		/// <returns></returns>
		private string NormalizePath(string path)
		{
			var result = path;

			if (!String.IsNullOrEmpty(path))
			{
				result = path.Replace("/", pathSeparator);
			}

			return result;
		}

		// NormalizePath

		/// <summary>
		/// 	Throws an argument exception stating that the specified path does
		/// 	not represent either a document or an index
		/// </summary>
		/// <param name = "path">path which is invalid</param>
		private void ThrowTerminatingInvalidPathException(string path)
		{
			var message = new StringBuilder("Path must represent either a document or an index:");
			message.Append(path);

			throw new ArgumentException(message.ToString());
		}

		private string RemoveDriveFromPath(string path)
		{
			var result = path;
			string root;

			if (this.PSDriveInfo == null)
			{
				root = String.Empty;
			}
			else
			{
				root = this.PSDriveInfo.Root;
			}

			if (result == null)
			{
				result = String.Empty;
			}

			if (result.Contains(root))
			{
				result = result.Replace(root, "");
			}

			return result;
		}

		#endregion
	}
}