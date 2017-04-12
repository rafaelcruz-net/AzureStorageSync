using System;
using System.Collections.Generic;
using System.Data.Services.Client;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Xml;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Data.OData;
using System.Xml.Linq;
using Microsoft.WindowsAzure.Storage.Table.DataServices;
using System.Text;

namespace StorageSync
{ 
    public class Backup
    {
        private CloudStorageAccount account;
        private CloudStorageAccount backupToAccount;
        private int maxThreads;
        private IEnumerable<string> priorityTables;
        private IEnumerable<string> priorityContainers;
        private IEnumerable<string> excludeTables;
        private IEnumerable<string> excludeContainers;

        private static XNamespace AtomNamespace = "http://www.w3.org/2005/Atom";
        private static XNamespace AstoriaDataNamespace = "http://schemas.microsoft.com/ado/2007/08/dataservices";
        private static XNamespace AstoriaMetadataNamespace = "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata";
        

        public Backup(CloudStorageAccount source,
            CloudStorageAccount destination,
            int maxThreads = 5,
            IEnumerable<string> priorityContainers = null,
            IEnumerable<string> excludeContainers = null)
        {
            this.account = source;
            this.backupToAccount = destination;
            this.maxThreads = maxThreads;
            this.priorityTables = priorityTables != null ? priorityTables : new List<string>();
            this.priorityContainers = priorityContainers != null ? priorityContainers : new List<string>();
            this.excludeTables = excludeTables != null ? excludeTables : new List<string>();
            this.excludeContainers = excludeContainers != null ? excludeContainers : new List<string>(); 
        }

        public void BeginBackup()
        {
            Trace.TraceInformation("{0:yyyy MM dd hh:mm:ss} Storage backup started.", DateTime.UtcNow);
            Console.WriteLine("{0:yyyy MM dd hh:mm:ss} Storage backup started.", DateTime.UtcNow);
            // Create the backup container
            var blobClient = StorageUtil.GetBlobClient(this.account);
            var backupToBlobClient = StorageUtil.GetBlobClient(this.backupToAccount);
      

            // Build the backup list
            var tempContainers = GetContainers(blobClient);
            List<String> containers = new List<string>();


            if (excludeContainers.Count() > 0)
            {

                //Exclude de container in list 
                foreach (var container in tempContainers)
                {
                    if (!excludeContainers.Any(x => x == container))
                        containers.Add(container);
                }
            }
            else
            {
                containers = tempContainers.ToList(); 
            }


            var backupList = StorageUtil.CreateRunList(containers, this.priorityContainers,  BackupContainer);
            
            var threadRunner = new ThreadRunner();
            threadRunner.Run(backupList, this.maxThreads);

            Trace.TraceInformation("{0:yyyy MM dd hh:mm:ss} Storage backup completed.", DateTime.UtcNow);
            Console.WriteLine("{0:yyyy MM dd hh:mm:ss} Storage backup completed.", DateTime.UtcNow);

            Console.WriteLine("Press any key to exit");
            Console.ReadKey();



        }


        private void BackupContainer(string containerName)
        {
            var blobClient = StorageUtil.GetBlobClient(this.account);
            var backupToBlobClient = StorageUtil.GetBlobClient(this.backupToAccount);

            var sourceContainer = blobClient.GetContainerReference(containerName);
            var destinationContainer = backupToBlobClient.GetContainerReference(containerName);

            if (destinationContainer.CreateIfNotExists())
            {
                destinationContainer.SetPermissions(new BlobContainerPermissions
                {
                    PublicAccess = BlobContainerPublicAccessType.Blob
                });
            }

            var blobs = sourceContainer.ListBlobs();

            Trace.TraceInformation("{0:yyyy MM dd hh:mm:ss} Backing up container {1} in thread {2}.", DateTime.UtcNow, containerName, Thread.CurrentThread.ManagedThreadId);
            Console.WriteLine("{0:yyyy MM dd hh:mm:ss} Backing up container {1} in thread {2}.", DateTime.UtcNow, containerName, Thread.CurrentThread.ManagedThreadId);

            foreach (var blob in blobs)
            {
                if (blob is CloudBlobDirectory)
                {
                    var srcBlobList = (blob as CloudBlobDirectory).ListBlobs(useFlatBlobListing: true, blobListingDetails: BlobListingDetails.None);

                    foreach (var src in srcBlobList)
                    {

                        string blobToken = blob.Container.GetSharedAccessSignature(new SharedAccessBlobPolicy
                        {
                            Permissions = SharedAccessBlobPermissions.Read | SharedAccessBlobPermissions.Write,
                            SharedAccessExpiryTime = DateTime.UtcNow + TimeSpan.FromDays(14)
                        });

                        var srcBlob = src as ICloudBlob;

                        CloudBlob destBlob;
                        if (srcBlob.Properties.BlobType == BlobType.BlockBlob)
                            destBlob = destinationContainer.GetBlockBlobReference(srcBlob.Name);
                        else
                            destBlob = destinationContainer.GetPageBlobReference(srcBlob.Name);

                        Console.WriteLine("Starting Copying Blob {0}", srcBlob.Name);

                        destBlob.BeginStartCopy(new Uri(srcBlob.Uri.AbsoluteUri + blobToken), null, null);

                        Console.WriteLine("Finished Copying Blob {0}", srcBlob.Name);
                        
                    }
                }
            }

            Trace.TraceInformation("{0:yyyy MM dd hh:mm:ss} Finished backing up container {1}", DateTime.UtcNow, containerName);
            Console.WriteLine("{0:yyyy MM dd hh:mm:ss} Finished backing up container {1}", DateTime.UtcNow, containerName);
        }


        private Stream BackupBlock(MemoryStream stream, Block block, out TableEntity firstEntity, out TableEntity lastEntity)
        {
            int entityCount = 0;
            firstEntity = null;
            lastEntity = null;

            // reset the memory stream as we begin a new block
            stream.Seek(0, SeekOrigin.Begin);
            stream.SetLength(0);
            XmlWriter writer = XmlWriter.Create(stream);

            writer.WriteStartElement("Block");
            foreach (Batch batch in block.Batches)
            {
                // write begin batch statement
                writer.WriteStartElement("Batch");
                foreach (BackupEntity entity in batch.Entities)
                {
                    entityCount++;
                    entity.EntryElement.WriteTo(writer);

                    if (firstEntity == null)
                    {
                        firstEntity = entity;
                    }

                    lastEntity = entity;
                }
                writer.WriteEndElement();

            }
            writer.WriteEndElement();
            writer.Close();
            stream.SetLength(stream.Position);
            stream.Seek(0, SeekOrigin.Begin);

            // if we have written > 0 entities, let us store to a block. Else we can reject this block
            if (entityCount > 0)
            {
                return stream;
            }

            return null;
        }


    

        private IEnumerable<string> GetContainers(CloudBlobClient blobClient)
        {
            // Get all blob containers for the specified account
            var containers = blobClient.ListContainers().Select(c => c.Name);

            return containers;
        }

 
        #region Internal Class & Methods
        /// <summary>
        /// The class that maintains the global state for the iteration
        /// </summary>
        internal class State
        {
            protected MemoryStream stream;
            IEnumerator<BackupEntity> queryIterator;

            internal State(IQueryable<BackupEntity> query, MemoryStream stream)
            {
                this.queryIterator = query.GetEnumerator();
                this.stream = stream;
            }

            /// <summary>
            /// This entity is the one we may have retrieved but it does not belong to the batch
            /// So we store it here so that it can be returned on the next iteration
            /// </summary>
            internal BackupEntity LookAheadEntity { private get; set; }

            /// <summary>
            /// We have completed if look ahead entity is null and iterator is completed too.
            /// </summary>
            internal bool HasCompleted
            {
                get
                {
                    return this.queryIterator == null && this.LookAheadEntity == null;
                }
            }

            /// <summary>
            /// Get the amount of data we have saved in the entity
            /// </summary>
            internal long CurrentBlockSize
            {
                get
                {
                    stream.Flush();
                    return stream.Position;
                }
            }

            /// <summary>
            /// Return the next entity - which can be either the 
            /// look ahead entity or a new one from the iterator.
            /// We return null if there are no more entities
            /// </summary>
            /// <returns></returns>
            internal BackupEntity GetNextEntity()
            {
                BackupEntity entityToReturn = null;
                if (this.LookAheadEntity != null)
                {
                    entityToReturn = this.LookAheadEntity;
                    this.LookAheadEntity = null;
                }
                else if (this.queryIterator != null)
                {
                    if (this.queryIterator.MoveNext())
                    {
                        entityToReturn = this.queryIterator.Current;
                    }
                    else
                    {
                        this.queryIterator = null;
                    }
                }

                return entityToReturn;
            }
        }

        /// <summary>
        /// Represents a collection of entities in a single batch
        /// </summary>
        internal class Batch
        {
            static int MaxEntityCount = 100;
            // Save at most 3.5MB in a batch so that we have enough room for 
            // the xml tags that WCF Data Services adds in the OData protocol
            static int MaxBatchSize = (int)(3.5 * 1024 * 1024);

            State state;

            internal Batch(State state)
            {
                this.state = state;
            }

            /// <summary>
            /// Yield entities until we hit a condition that should terminate a batch.
            /// The conditions to terminate on are:
            /// 1. 100 entities in a batch
            /// 2. 3.5MB of data
            /// 2. 3.8MB of block size
            /// 3. We see a new partition key
            /// </summary>
            internal IEnumerable<BackupEntity> Entities
            {
                get
                {
                    BackupEntity entity;
                    long currentSize = this.state.CurrentBlockSize;

                    string lastPartitionKeySeen = null;
                    int entityCount = 0;

                    while ((entity = state.GetNextEntity()) != null)
                    {
                        if (lastPartitionKeySeen == null)
                        {
                            lastPartitionKeySeen = entity.PartitionKey;
                        }

                        int approxEntitySize = entity.EntryElement.ToString().Length * 2;
                        long batchSize = this.state.CurrentBlockSize - currentSize;
                        if (entityCount >= Batch.MaxEntityCount
                            || !string.Equals(entity.PartitionKey, lastPartitionKeySeen)
                            || batchSize + approxEntitySize > Batch.MaxBatchSize
                            || this.state.CurrentBlockSize + approxEntitySize > Block.MaxBlockSize)
                        {
                            // set this current entity as the look ahead since it needs to be part of the next batch
                            state.LookAheadEntity = entity;                      
                            yield break;
                        }

                        entityCount++;
                        yield return entity;
                    }
                }
            }
        }

        /// <summary>
        /// Represents all batches in a block
        /// </summary>
        internal class Block
        {
            // Though a block can be of 4MB we will stop before to allow buffer
            public static int MaxBlockSize = (int)(3.8 * 1024 * 1024);

            State state;

            internal string BlockId { get; private set; }

            internal Block(State state)
            {
                this.state = state;
                this.BlockId = Convert.ToBase64String(Guid.NewGuid().ToByteArray());
            }

            /// <summary>
            /// The list of batches in the block. 
            /// </summary>
            internal IEnumerable<Batch> Batches
            {
                get
                {
                    while (!state.HasCompleted && state.CurrentBlockSize < Block.MaxBlockSize)
                    {
                        yield return new Batch(state);
                    }
                }
            }
        }

        /// <summary>
        /// Represents all blocks in a blob
        /// </summary>
        internal class Blob
        {
            /// <summary>
            /// We will allow storing at most 20 blocks in a blob
            /// </summary>
            static int MaxBlocksInBlobs = 20;

            State state;
            internal CloudBlockBlob blob { get; private set; }

            internal Blob(State state)
            {
                this.state = state;
            }

            /// <summary>
            /// The blocks that form the blob
            /// </summary>
            internal IEnumerable<Block> Blocks
            {
                get
                {
                    int blockCount = 0;

                    while (!state.HasCompleted && blockCount < Blob.MaxBlocksInBlobs)
                    {
                        blockCount++;
                        yield return new Block(state);
                    }
                }
            }
        }

        private static Type ResolveType(string entityName)
        {
            return typeof(BackupEntity);
        }

        private static void OnReadingEntity(object sender, ReadingWritingEntityEventArgs args)
        {
            BackupEntity entity = args.Entity as BackupEntity;
            entity.EntryElement = args.Data;
        }

        private static void OnWritingEntity(object sender, ReadingWritingEntityEventArgs args)
        {
            BackupEntity entity = args.Entity as BackupEntity;
            XElement content = args.Data.Element(AtomNamespace + "content");
            XElement propertiesElem = content.Element(AstoriaMetadataNamespace + "properties");

            propertiesElem.Remove();

            XElement propertiesElemToUse = entity.EntryElement.Elements(AtomNamespace + "content")
                                            .Elements(AstoriaMetadataNamespace + "properties")
                                            .FirstOrDefault();

            content.Add(propertiesElemToUse);
        }
    }

    #endregion
}