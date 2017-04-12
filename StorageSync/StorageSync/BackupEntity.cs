using System.Xml.Linq;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Table.DataServices;
using System.Data.Services.Client;
using System.Data.Services.Common;

namespace StorageSync
{
    [DataServiceKey("PartitionKey", "RowKey")]
    internal class BackupEntity : TableEntity
    {
        internal XElement EntryElement { get; set; }
    }
}
