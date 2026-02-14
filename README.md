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

## License

[MIT](LICENSE) -- Copyright (c) 2018 Dimo Terziev
