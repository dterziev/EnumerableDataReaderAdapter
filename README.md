# EnumerableDataReaderAdapter

An adapter that allows converting an IEnumerable<T> to IDataReader that can be used as a data source for SqlBulkCopy.

## Usage

```c#
var data = Enumerable.Range(1, 10000).Select(x => new { Id = x, Name = $"name-{x}" });

using var reader = data.ToDataReader(map => map.Add(x => x.Id).Add(x => x.Name));

using var sqlBulkCopy = new SqlBulkCopy(connectionString);
sqlBulkCopy.BatchSize = 10000;
sqlBulkCopy.ColumnMappings.Add("Id", "Id");
sqlBulkCopy.ColumnMappings.Add("Name", "Name");
sqlBulkCopy.DestinationTableName = "dbo.TableName";
sqlBulkCopy.EnableStreaming = true;
await sqlBulkCopy.WriteToServerAsync(reader);
```