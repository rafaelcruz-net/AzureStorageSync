using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.WindowsAzure;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System.IO;

namespace StorageSync
{
    internal static class StorageUtil
    {
        public static CloudBlobClient GetBlobClient(CloudStorageAccount account)
        {
            var blobClient = new CloudBlobClient(new StorageUri(account.BlobEndpoint), account.Credentials);

          
            return blobClient;
        }

        public static CloudTableClient GetTableClient(CloudStorageAccount account)
        {
            var tableClient = new CloudTableClient(new StorageUri(account.TableEndpoint), account.Credentials);

            return tableClient;
        }

        public static void CopyBlob(CloudBlockBlob sourceBlob, CloudBlockBlob destBlob)
        {
            // If the accounts are the same then use CopyFromBlob for effiency
            if (sourceBlob.Uri.Host == destBlob.Uri.Host)
            {
                destBlob.StartCopy(sourceBlob);
            }
            else
            {
                // CopyFromBlob does not work across storage accounts, so we must open and re-upload the blob stream
                using (Stream stream = sourceBlob.OpenRead())
                {
                    destBlob.OpenWrite();
                    destBlob.UploadFromStream(stream);
                }
            }
        }

        public static IEnumerable<ThreadRunner.ThreadRunnerThread> CreateRunList(IEnumerable<string> containers,
            IEnumerable<string> priorityContainers,
            ThreadRunner.TaskDelegate containerDelegate)
        {
            // Build consolidated list
            return  (from c in containers
                    join j in priorityContainers on c equals j into outer
                    from j in outer.DefaultIfEmpty()
                    select new ThreadRunner.ThreadRunnerThread(c, containerDelegate, j != null ? 1 : 3)).OrderBy(p => p.Priority).ToList();
        }
    }
}