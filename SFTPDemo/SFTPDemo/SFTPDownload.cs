using System;
using System.Configuration;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Polly;
using Renci.SshNet;
using Renci.SshNet.Sftp;

namespace SFTPDemo
{
	public class SFTPMessage : TableEntity
	{
		public DateTime CreatedDate { get; set; }
		public string FolderID { get; set; }
		public string FileName { get; set; }
		public string FolderName { get; set; }

	}

	public class WorkItemDetails
	{
		public string FolderID { get; set; }
		public string FileName { get; set; }
		public string FolderName { get; set; }

	}
	public static class SFTPDownload
    {

		private static HttpClient httpClient = new HttpClient();

		[FunctionName("SFTPDownload")]
        public static async Task Run([TimerTrigger("0 */3 * * * *")]TimerInfo myTimer,
			[Table( "SFTP", Connection = "AzureWebJobsStorage" )] CloudTable cloudTable,
			TraceWriter log)
        {
			log.Info( $"C# Timer trigger function executed at: {DateTime.Now}" );

			//Connection to the sftp
			var host = ConfigurationManager.AppSettings[ "SftpHost" ];
			var username = ConfigurationManager.AppSettings[ "sftpUsername" ];
			var password = ConfigurationManager.AppSettings[ "SftpPassword" ];
			var port = Convert.ToInt32(ConfigurationManager.AppSettings[ "SftpPort" ]);
			var workingdirectory = ConfigurationManager.AppSettings[ "SftpDir" ];

			//Connect to Storage
			var containerName = ConfigurationManager.AppSettings[ "ContainerName" ];
			var storageConnectionString = ConfigurationManager.AppSettings[ "StorageConnectionString" ];

			CloudStorageAccount cloudStorageAccount = CloudStorageAccount.Parse( storageConnectionString );
			CloudBlobClient blobClient = cloudStorageAccount.CreateCloudBlobClient();
			CloudBlobContainer container = blobClient.GetContainerReference( containerName );
			await container.CreateIfNotExistsAsync();

			try
			{
				using( var client = new SftpClient( host, port, username, password ) )
				{
					client.Connect();
					if( !client.IsConnected )
					{
						log.Error( "Error while connecting to SFTP server" );
						Console.WriteLine( "Error while connecting to SFTP server" );
					}
					client.ChangeDirectory( workingdirectory );
					var listDirectory = client.ListDirectory( workingdirectory );

					foreach( var file in listDirectory )
					{

						if( Path.GetExtension( file.Name ).Equals( ".zip" ) )
						{
							string strFolder = Path.GetFileNameWithoutExtension( file.Name );
							if( await DownloadZip( client, workingdirectory, file, container, cloudTable, strFolder, log ) )
								file.Delete();
							else
							{
								log.Error( "Error while downloading the file " + file.Name );
							}

						}
						else if( file.IsSymbolicLink )
						{
							log.Error( "Symbolic link ignored: " + file.FullName );
						}
					}
				}
			}
			catch( Exception ex)
			{
				log.Error( "Error occured while processing... ", ex );
				throw;
			}
		}

		private static async Task<bool> DownloadZip( SftpClient client, string source, SftpFile file, CloudBlobContainer container, CloudTable table, string folderName, TraceWriter log )
		{
			string folderID = Guid.NewGuid().ToString();
			try
			{
				
				//Read the zip through all entries
				log.Info( $"Processing zip file {file.Name}" );

				string remoteFileName = file.Name;
				byte[] buffer = client.ReadAllBytes( source + "/" + remoteFileName );
				MemoryStream ms = new MemoryStream( buffer );
				using( ZipArchive zip = new ZipArchive( ms, ZipArchiveMode.Read ) )
				{
					foreach( ZipArchiveEntry entry in zip.Entries )
					{
						if( entry.Name.EndsWith( ".DS_Store", StringComparison.InvariantCultureIgnoreCase ) ) continue;
						using( var stream = entry.Open() )
						{
							await DownloadFile( stream, entry.Name, container, table, folderName, folderID, log );
						}
					}
				}

				var NewItem = new WorkItemDetails();
				NewItem.FileName = file.Name;
				NewItem.FolderName = folderName;
				NewItem.FolderID = folderID;

				string JSONresult = JsonConvert.SerializeObject( NewItem );
				var workflowRepsonse = await CreateWorkItem( JSONresult, file.Name, log );
				if( workflowRepsonse.Item1 )
					log.Info( "Item Created with Id :" + workflowRepsonse.Item2 );
				else
				{
					log.Info( $"Item could not be created.Deleting Blobs for folder {folderID}" );
					await DeleteAllFilesFromBlob( container, folderID );
				}
				return true;
			}
			catch(Exception ex)
			{
				log.Error( "Error while processing zip file. " + ex.Message );
				await DeleteAllFilesFromBlob( container, folderID );
				return false;
			}
		}
		private static async Task DownloadFile( Stream fileStream, string file, CloudBlobContainer container, CloudTable table, string folderName, string folderID, TraceWriter log )
		{
			Console.WriteLine( "Downloading {0}", file );

			string fileGuID = Guid.NewGuid().ToString();

			string strBlobItemName = file;
			if( !string.IsNullOrEmpty( folderName ) )
				strBlobItemName = folderID + "\\" + file;

			var NewItem = new SFTPMessage();
			NewItem.CreatedDate = DateTime.Now;
			NewItem.FileName = file;
			NewItem.FolderName = folderName;
			NewItem.FolderID = folderID;
			NewItem.PartitionKey = "SFTP";
			NewItem.RowKey = fileGuID;
			NewItem.ETag = "*";

			try
			{
				TableOperation insertOperation = TableOperation.Insert( NewItem );
				await table.ExecuteAsync( insertOperation );

				CloudBlockBlob blob = container.GetBlockBlobReference( strBlobItemName );
				await blob.UploadFromStreamAsync( fileStream );
				log.Info( $"File {file} saved successfully" );
			}
			catch( Exception ex )
			{
				log.Info( $"File not saved successfully" );
				throw;
			}
			Console.WriteLine( "Saved {0}", file );
		}

		public static async Task<Tuple<bool, string>> CreateWorkItem(string Result, string workItem, TraceWriter log )
		{
			try
			{
				//Create the workitem in workflow
				log.Info( $"Initiate the workflow..." );
				var workflowurl = ConfigurationManager.AppSettings[ "workflowurl" ];
				var workflowId = ConfigurationManager.AppSettings[ "workflowId" ];
				var workType = ConfigurationManager.AppSettings[ "workType" ];
				var url = workflowurl + @"?workflowId=" + workflowId + @"&workType=" + workType;
				return await PostMessageAsync( url, Result, log );
			}
			catch(Exception ex)
			{
				throw;
			}
		}

		public static async Task<Tuple<bool, string>> PostMessageAsync( string url, string messageAsJson, TraceWriter log )
		{
			var content = new System.Net.Http.StringContent( messageAsJson, System.Text.Encoding.UTF8, "application/json" );
			// Prepare the retry policy
			var response = await Policy
				.HandleResult<HttpResponseMessage>( message => !message.IsSuccessStatusCode )
				.WaitAndRetryAsync( 3, i => TimeSpan.FromSeconds( 2 ), ( result, timeSpan, retryCount, context ) =>
				{
					log.Warning( $"Request failed with {result.Result.StatusCode}. Waiting {timeSpan} before next retry. Retry attempt {retryCount}" );
					Console.WriteLine( "Retrying..." );
				} )
				.ExecuteAsync( () => httpClient.PostAsync( new Uri( url ), content ) );

			bool sucessStatusCode = response.IsSuccessStatusCode;
			string wfResponse = await response.Content.ReadAsStringAsync();

			Tuple<bool, string> returnValue = new Tuple<bool, string>( sucessStatusCode, wfResponse );
			return returnValue;

		}

		public static async Task DeleteAllFilesFromBlob( CloudBlobContainer _blobContainer, string folderID )
		{

			var ctoken = new BlobContinuationToken();
			do
			{
				var result = await _blobContainer.ListBlobsSegmentedAsync( folderID, true, BlobListingDetails.None, null, ctoken, null, null );
				ctoken = result.ContinuationToken;
				await Task.WhenAll( result.Results
					.Select( item => ( item as CloudBlob )?.DeleteIfExistsAsync() )
					.Where( task => task != null )
				);
			} while( ctoken != null );
		}

	}
}
