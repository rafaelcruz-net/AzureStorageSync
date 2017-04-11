# Azure Storage Sync
Azure Storage Sync is a api that allow you to copy one blob storage to another blob storage. 
Simple to use, just pass the source and the target and the library will do the hard work.

## See How Work

### Simple Usage

```
static void Main(string[] args)
{
   var source = CloudStorageAccount.Parse(<AccountStorage>);
   var dest = CloudStorageAccount.Parse(<AccountStorage>);
   var backup = new Backup(account:source, backupToAccount:dest, maxThreads:5);
   backup.BeginBackup();
}
```
### Exclude Containers

```
static void Main(string[] args)
{
   var source = CloudStorageAccount.Parse(<AccountStorage>);
   var dest = CloudStorageAccount.Parse(<AccountStorage>);
   var excludeContainers = new string[] { "$logs" };
   var backup = new Backup(account:source, backupToAccount:dest, maxThreads:5, excludeContainers:excludeContainers);
   backup.BeginBackup();
}
```
