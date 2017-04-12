using Microsoft.WindowsAzure.Storage;
using StorageSync;
using System;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace StorageSyncSample
{
    class Program
    {
        static void Main(string[] args)
        {
            var source = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["Source"]);
            var dest = CloudStorageAccount.Parse(ConfigurationManager.AppSettings["Target"]);
            
            var excludeContainers = new string[] { "$logs", "vsdeploy" };

            var backup = new Backup(source, dest, excludeContainers: excludeContainers);

            backup.BeginBackup();
        }
    }
}
