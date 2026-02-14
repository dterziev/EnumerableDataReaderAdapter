# EnumerableDataReaderAdapter

A lightweight .NET library that converts `IEnumerable<T>` into an `IDataReader`, enabling streaming of in-memory collections into APIs that require `IDataReader` -- most notably `SqlBulkCopy` for high-performance bulk inserts into SQL Server.

## Features

- **Streaming** -- rows are read lazily from the enumerable; the entire collection is never buffered in memory.
- **Automatic property mapping** -- public properties are discovered automatically when no explicit mapping is provided.
- **Fluent column mapping API** -- choose exactly which columns to expose using expression-based or delegate-based mappings.
- **Computed columns** -- map constant values or derived expressions that don't correspond to a property.
- **Multi-target** -- supports .NET 8.0, .NET 9.0, and .NET 10.0.

## Installation

Add a project reference or include the source in your solution. The library has no external runtime dependencies.

## Usage

### Basic usage with SqlBulkCopy

```csharp
var data = Enumerable.Range(1, 10_000)
    .Select(x => new { Id = x, Name = $"name-{x}" });

using var reader = data.ToDataReader(map => map
    .Add(x => x.Id)
    .Add(x => x.Name));

using var bulkCopy = new SqlBulkCopy(connectionString);
bulkCopy.DestinationTableName = "dbo.People";
bulkCopy.EnableStreaming = true;
bulkCopy.ColumnMappings.Add("Id", "Id");
bulkCopy.ColumnMappings.Add("Name", "Name");

await bulkCopy.WriteToServerAsync(reader);
```

### Default mapping (auto-discover all public properties)

If no mapping is configured, every public instance property on `T` is exposed as a column:

```csharp
var reader = products.ToDataReader();
```

### Expression-based mapping

Use lambda expressions that point to properties. The column name and type are inferred automatically:

```csharp
var reader = products.ToDataReader(map => map
    .Add(p => p.Id)
    .Add(p => p.Name)
    .Add(p => p.Price));
```

### Delegate-based mapping with explicit name and type

For full control -- including computed/constant columns -- specify the column name, CLR type, and a value delegate:

```csharp
var reader = orders.ToDataReader(map => map
    .Add("OrderId", typeof(int), o => o.Id)
    .Add("Total",   typeof(decimal), o => o.Quantity * o.UnitPrice)
    .Add("Source",  typeof(string), _ => "Import"));
```

### Mixing mapping styles

The two `Add` overloads can be freely combined in a single mapping configuration:

```csharp
var reader = items.ToDataReader(map => map
    .Add(i => i.Id)
    .Add("DisplayName", typeof(string), i => $"{i.FirstName} {i.LastName}")
    .Add(i => i.CreatedAt));
```

## API Reference

### `EnumerableExtensions`

| Method | Description |
|--------|-------------|
| `ToDataReader<T>(this IEnumerable<T>, Action<ColumnMappings<T>>?)` | Creates an `IDataReader` with optional mapping configuration. When no columns are configured, all public properties of `T` are used. |
| `ToDataReader<T>(this IEnumerable<T>, ColumnMappings<T>)` | Creates an `IDataReader` using a pre-built `ColumnMappings<T>` instance. |

### `ColumnMappings<T>`

| Method | Description |
|--------|-------------|
| `Add(Expression<Func<T, object?>>)` | Adds a column from a property expression. Column name and type are inferred from the member. |
| `Add(string, Type, Func<T, object?>)` | Adds a column with an explicit name, CLR type, and value delegate. |

Both `Add` methods return `this`, so calls can be chained fluently.

## Building

```bash
dotnet build
```

## Running tests

```bash
dotnet test
```

## Benchmarks

```bash
dotnet run --project benchmarks/EnumerableDataReaderAdapter.Benchmarks -c Release
```

### Results

The benchmarks measure reading 10,000 rows through the `IDataReader` interface using different mapping strategies and column access patterns across .NET 8.0, 9.0, and 10.0.

```
BenchmarkDotNet v0.15.8, Linux Ubuntu 24.04.3 LTS (Noble Numbat)
unknown 2.10GHz, 1 CPU, 16 logical and 16 physical cores
.NET SDK 10.0.103
  [Host]    : .NET 10.0.3 (10.0.3, 10.0.326.7603), X64 RyuJIT x86-64-v4
  .NET 10.0 : .NET 10.0.3 (10.0.3, 10.0.326.7603), X64 RyuJIT x86-64-v4
  .NET 8.0  : .NET 8.0.24 (8.0.24, 8.0.2426.7010), X64 RyuJIT x86-64-v4
  .NET 9.0  : .NET 9.0.13 (9.0.13, 9.0.1326.6317), X64 RyuJIT x86-64-v4
```

| Method                                         | Job       | Runtime   | N     | Mean     | Error     | StdDev    | Ratio | RatioSD | Rank | Gen0    | Allocated | Alloc Ratio |
|----------------------------------------------- |---------- |---------- |------ |---------:|----------:|----------:|------:|--------:|-----:|--------:|----------:|------------:|
| DefaultMapping_ByColumnIndex                   | .NET 10.0 | .NET 10.0 | 10000 | 1.982 ms | 0.0376 ms | 0.0462 ms |  1.00 |    0.03 |    1 | 85.9375 | 351.82 KB |        1.00 |
| MappingExpressions_ByColumnIndex               | .NET 10.0 | .NET 10.0 | 10000 | 1.902 ms | 0.0380 ms | 0.0842 ms |  0.96 |    0.05 |    1 | 85.9375 | 366.11 KB |        1.04 |
| MappingExpressions_ByColumnIndex_CachedMapping | .NET 10.0 | .NET 10.0 | 10000 | 1.948 ms | 0.0363 ms | 0.0372 ms |  0.98 |    0.03 |    1 | 85.9375 | 351.88 KB |        1.00 |
| MappingDelegates_ByColumnIndex                 | .NET 10.0 | .NET 10.0 | 10000 | 1.968 ms | 0.0392 ms | 0.0524 ms |  0.99 |    0.03 |    1 | 85.9375 | 352.13 KB |        1.00 |
| MappingDelegates_ByColumnIndex_CachedMapping   | .NET 10.0 | .NET 10.0 | 10000 | 2.003 ms | 0.0396 ms | 0.0826 ms |  1.01 |    0.05 |    1 | 85.9375 | 351.88 KB |        1.00 |
| DefaultMapping_ByColumnName                    | .NET 10.0 | .NET 10.0 | 10000 | 2.034 ms | 0.0398 ms | 0.0458 ms |  1.03 |    0.03 |    1 | 85.9375 | 352.03 KB |        1.00 |
| MappingExpressions_ByColumnName                | .NET 10.0 | .NET 10.0 | 10000 | 2.301 ms | 0.0457 ms | 0.0836 ms |  1.16 |    0.05 |    2 | 89.8438 | 366.32 KB |        1.04 |
| MappingExpressions_ByColumnName_CachedMapping  | .NET 10.0 | .NET 10.0 | 10000 | 2.041 ms | 0.0405 ms | 0.0845 ms |  1.03 |    0.05 |    1 | 85.9375 | 352.09 KB |        1.00 |
| MappingDelegates_ByColumnName                  | .NET 10.0 | .NET 10.0 | 10000 | 1.995 ms | 0.0390 ms | 0.0534 ms |  1.01 |    0.04 |    1 | 85.9375 | 352.34 KB |        1.00 |
| MappingDelegates_ByColumnName_CachedMapping    | .NET 10.0 | .NET 10.0 | 10000 | 2.032 ms | 0.0406 ms | 0.0655 ms |  1.03 |    0.04 |    1 | 85.9375 | 352.09 KB |        1.00 |
|                                                |           |           |       |          |           |           |       |         |      |         |           |             |
| DefaultMapping_ByColumnIndex                   | .NET 8.0  | .NET 8.0  | 10000 | 2.119 ms | 0.0421 ms | 0.0517 ms |  1.00 |    0.03 |    1 | 85.9375 | 351.84 KB |        1.00 |
| MappingExpressions_ByColumnIndex               | .NET 8.0  | .NET 8.0  | 10000 | 2.241 ms | 0.0423 ms | 0.0893 ms |  1.06 |    0.05 |    1 | 85.9375 | 366.11 KB |        1.04 |
| MappingExpressions_ByColumnIndex_CachedMapping | .NET 8.0  | .NET 8.0  | 10000 | 2.102 ms | 0.0414 ms | 0.0736 ms |  0.99 |    0.04 |    1 | 85.9375 | 351.88 KB |        1.00 |
| MappingDelegates_ByColumnIndex                 | .NET 8.0  | .NET 8.0  | 10000 | 2.068 ms | 0.0378 ms | 0.0681 ms |  0.98 |    0.04 |    1 | 85.9375 | 352.42 KB |        1.00 |
| MappingDelegates_ByColumnIndex_CachedMapping   | .NET 8.0  | .NET 8.0  | 10000 | 2.109 ms | 0.0415 ms | 0.0608 ms |  1.00 |    0.04 |    1 | 85.9375 | 351.88 KB |        1.00 |
| DefaultMapping_ByColumnName                    | .NET 8.0  | .NET 8.0  | 10000 | 2.671 ms | 0.0533 ms | 0.0829 ms |  1.26 |    0.05 |    2 | 85.9375 | 352.05 KB |        1.00 |
| MappingExpressions_ByColumnName                | .NET 8.0  | .NET 8.0  | 10000 | 2.808 ms | 0.0558 ms | 0.1272 ms |  1.33 |    0.07 |    2 | 89.8438 | 366.32 KB |        1.04 |
| MappingExpressions_ByColumnName_CachedMapping  | .NET 8.0  | .NET 8.0  | 10000 | 2.610 ms | 0.0512 ms | 0.0701 ms |  1.23 |    0.04 |    2 | 85.9375 | 352.09 KB |        1.00 |
| MappingDelegates_ByColumnName                  | .NET 8.0  | .NET 8.0  | 10000 | 2.486 ms | 0.0476 ms | 0.0962 ms |  1.17 |    0.05 |    2 | 85.9375 | 352.63 KB |        1.00 |
| MappingDelegates_ByColumnName_CachedMapping    | .NET 8.0  | .NET 8.0  | 10000 | 2.557 ms | 0.0504 ms | 0.0690 ms |  1.21 |    0.04 |    2 | 85.9375 | 352.09 KB |        1.00 |
|                                                |           |           |       |          |           |           |       |         |      |         |           |             |
| DefaultMapping_ByColumnIndex                   | .NET 9.0  | .NET 9.0  | 10000 | 2.064 ms | 0.0391 ms | 0.0434 ms |  1.00 |    0.03 |    1 | 85.9375 | 351.82 KB |        1.00 |
| MappingExpressions_ByColumnIndex               | .NET 9.0  | .NET 9.0  | 10000 | 2.272 ms | 0.0411 ms | 0.0831 ms |  1.10 |    0.05 |    2 | 85.9375 | 366.11 KB |        1.04 |
| MappingExpressions_ByColumnIndex_CachedMapping | .NET 9.0  | .NET 9.0  | 10000 | 2.092 ms | 0.0404 ms | 0.0579 ms |  1.01 |    0.03 |    1 | 85.9375 | 351.88 KB |        1.00 |
| MappingDelegates_ByColumnIndex                 | .NET 9.0  | .NET 9.0  | 10000 | 2.056 ms | 0.0405 ms | 0.0643 ms |  1.00 |    0.04 |    1 | 85.9375 | 352.31 KB |        1.00 |
| MappingDelegates_ByColumnIndex_CachedMapping   | .NET 9.0  | .NET 9.0  | 10000 | 2.085 ms | 0.0409 ms | 0.0757 ms |  1.01 |    0.04 |    1 | 85.9375 | 351.88 KB |        1.00 |
| DefaultMapping_ByColumnName                    | .NET 9.0  | .NET 9.0  | 10000 | 2.601 ms | 0.0502 ms | 0.1080 ms |  1.26 |    0.06 |    3 | 85.9375 | 352.03 KB |        1.00 |
| MappingExpressions_ByColumnName                | .NET 9.0  | .NET 9.0  | 10000 | 2.645 ms | 0.0515 ms | 0.0688 ms |  1.28 |    0.04 |    3 | 89.8438 | 366.32 KB |        1.04 |
| MappingExpressions_ByColumnName_CachedMapping  | .NET 9.0  | .NET 9.0  | 10000 | 2.547 ms | 0.0474 ms | 0.0752 ms |  1.23 |    0.04 |    3 | 85.9375 | 352.09 KB |        1.00 |
| MappingDelegates_ByColumnName                  | .NET 9.0  | .NET 9.0  | 10000 | 2.468 ms | 0.0490 ms | 0.0967 ms |  1.20 |    0.05 |    3 | 85.9375 | 352.52 KB |        1.00 |
| MappingDelegates_ByColumnName_CachedMapping    | .NET 9.0  | .NET 9.0  | 10000 | 2.481 ms | 0.0486 ms | 0.0982 ms |  1.20 |    0.05 |    3 | 85.9375 | 352.09 KB |        1.00 |

## License

[MIT](LICENSE) -- Copyright (c) 2018 Dimo Terziev
